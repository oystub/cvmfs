package rest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/localdb"
	"github.com/cvmfs/ducc/scheduler"
)

const wish_path = "/wishes"

var db_ localdb.LocalDb

type ctxKey struct{}

func getField(r *http.Request, index int) string {
	fields := r.Context().Value(ctxKey{}).([]string)
	return fields[index]
}

func (this *PatternHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var allow []string
	for _, route := range this.routes {
		matches := route.pattern.FindStringSubmatch(r.URL.Path)
		if len(matches) > 0 {
			if r.Method != route.method {
				allow = append(allow, route.method)
				continue
			}
			ctx := context.WithValue(r.Context(), ctxKey{}, matches[1:])
			route.handler(w, r.WithContext(ctx))
			return
		}
	}
	if len(allow) > 0 {
		w.Header().Set("Allow", strings.Join(allow, ", "))
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.NotFound(w, r)
}

func Init() {
	db_.Init("ducc.db")
}

type route struct {
	method  string
	pattern *regexp.Regexp
	handler http.HandlerFunc
}

func NewRoute(method, pattern string, handler http.HandlerFunc) route {
	// Precompile patterns for better performance
	return route{method, regexp.MustCompile("^" + pattern + "$"), handler}
}

type PatternHandler struct {
	routes []route
}

func RunRawRestApi() {
	handler := PatternHandler{[]route{
		// Wishes
		NewRoute("GET", "/wishes", getAllWishesHandler),
		NewRoute("POST", "/wishes", createWishHandler),
		NewRoute("POST", "/wishes/([0-9]+)/sync", notImplementedHandler),
		NewRoute("GET", "/wishes/([0-9]+)", getWishHandler),
		NewRoute("DELETE", "/wishes/([0-9]+)", notImplementedHandler),
		NewRoute("POST", "/wishes/([0-9]+)/sync", notImplementedHandler),
		NewRoute("GET", "/wishes/([0-9]+)/images", notImplementedHandler),
		NewRoute("GET", "/wishes/([0-9]+)/jobs", notImplementedHandler),

		// Images
		NewRoute("GET", "/images", notImplementedHandler),
		NewRoute("GET", "/images/([0-9]+)", notImplementedHandler),
		NewRoute("POST", "/images/([0-9]+)/delete", notImplementedHandler),
		NewRoute("POST", "/images/([0-9]+)/sync", notImplementedHandler),
		NewRoute("GET", "/images/([0-9]+)/jobs", notImplementedHandler),

		// Layers
		// Not sure if we need this

		// Jobs
		NewRoute("GET", "/jobs", notImplementedHandler),
		NewRoute("GET", "/jobs/([0-9]+)", notImplementedHandler),
		NewRoute("POST", "/jobs/([0-9]+)/cancel", notImplementedHandler),

		// Recipes
		NewRoute("POST", "/recipes/.+", applyRecipeHandler), // OK

		// Webhooks
		NewRoute("POST", "/webhooks/harbor", notImplementedHandler),

		// Other general actions
		// - Clean up orphaned images
		// - Clean up orphaned layers

	}}
	http.HandleFunc("/", handler.ServeHTTP)
	http.ListenAndServe(":8080", nil)
}

func notImplementedHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte("Not implemented"))
}

// Endpoint for GET `/wishes/<wish-id>`
func getWishHandler(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(getField(r, 0), 10, 64)
	wish, err := db_.GetWishById(lib.ObjectId(id))
	if err == sql.ErrNoRows {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("Wish not found"))
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	wishJson, err := json.Marshal(WishFromWish2(wish))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(wishJson)
	return
}

// Endpoint for GET `/wishes`
func getAllWishesHandler(w http.ResponseWriter, r *http.Request) {
	// If no ID is given, return all wishes
	wishes, err := scheduler.GetAllWishes()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// Convert from internal type to API type
	restWishes := make([]Wish, len(wishes))
	for i, wish := range wishes {
		restWishes[i] = WishFromWish2(wish)
	}
	wishesJson, err := json.Marshal(restWishes)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	// Write JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(wishesJson)
	return
}

func createWishHandler(w http.ResponseWriter, r *http.Request) {
	// Parse the wish from the request body
	var inputWish CreateWish
	err := json.NewDecoder(r.Body).Decode(&inputWish)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid wish"))
		return
	}
	// Add the wish to the scheduler
	createdWish, err := scheduler.AddOrUpdateWish(inputWish.ToWish2())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// Send the new wish back to the client
	wishJson, err := json.Marshal(WishFromWish2(createdWish))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(wishJson)
}

func applyRecipeHandler(w http.ResponseWriter, r *http.Request) {
	source := getField(r, 0)

	// TODO: Validate source string

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Parse the recipe
	recipe, err := db.ParseYamlRecipeV1(body, source)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Invalid recipe: %s", err.Error())))
		return
	}

	// Import the recipe
	newWishes, deletedWishes, err := db.ImportRecipeV1(recipe)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// TODO: Schedule update of the new wishes

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Successfully applied recipe. Imported %d wish(es), deleted %d wish(es)", len(newWishes), len(deletedWishes))))
}
