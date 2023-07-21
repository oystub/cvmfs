package lib

import (
	"fmt"
	"net/http"
	"sync"
)

type ContainerRegistryCredentials struct {
	Username string
	Password string
}

type ContainerRegistryIdentifier struct {
	Schema string
	Host   string
	//port string TODO: Determine if this is needed
	//proxy string TODO: Determine if this is needed

}

type ContainerRegistry struct {
	Identifier ContainerRegistryIdentifier

	// Authentication
	Credentials     ContainerRegistryCredentials
	TokenCv         *sync.Cond
	token           string
	gotToken        bool
	waitingForToken bool
	// TODO: Some kind of auth error variable that can be checked
	// Number of simultaneous connections to the registry

	Client *http.Client
}

func (cr ContainerRegistry) baseUrl() string {
	return fmt.Sprintf("%s://%s/v2", cr.Identifier.Schema, cr.Identifier.Host)
}

func (cr *ContainerRegistry) GetToken() string {
	return cr.token
}

func (cr *ContainerRegistry) RequestAuthToken(url string) error {
	// Need to make a request to the registry to get the token
	return nil
}

func (cr *ContainerRegistry) waitUntilReadyToPerformRequest() {
	// If we are waiting for a new token, hold the request until we get one
	cr.TokenCv.L.Lock()
	for cr.waitingForToken {
		cr.TokenCv.Wait()
	}
	cr.TokenCv.L.Unlock()
}

func (cr *ContainerRegistry) PerformRequest(req *http.Request) (*http.Response, error) {
retryRequest:
	cr.waitUntilReadyToPerformRequest()

	// If we have a token, add it to the request
	cr.TokenCv.L.Lock()
	tokenToSend := cr.token
	if cr.gotToken {
		req.Header.Set("Authorization", tokenToSend)
	}
	cr.TokenCv.L.Unlock()

	// Perform the request
	res, err := cr.Client.Do(req)
	if err != nil {
		// Error performing request
		return nil, err
	}

	// We got a good response, everything is fine
	if res.StatusCode < 300 && res.StatusCode >= 200 {
		// TODO: logging
		return res, nil
	}

	// We are rate limited
	if res.StatusCode == http.StatusTooManyRequests {
		res.Body.Close()
		// TODO: Handle rate limit wait
		goto retryRequest
	}
	if res.StatusCode == http.StatusUnauthorized {
		WwwAuthenticate := res.Header["Www-Authenticate"][0]
		res.Body.Close()

		cr.TokenCv.L.Lock()
		if cr.waitingForToken || tokenToSend != cr.token {
			// Another thread has already requested a new token
			cr.TokenCv.L.Unlock()
			goto retryRequest
		}

		// We need to request a new token
		cr.waitingForToken = true
		cr.TokenCv.L.Unlock()

		token, err := requestAuthToken(WwwAuthenticate, cr.Credentials.Username, cr.Credentials.Password)
		if err != nil {
			// As a last resort, try again with no username and password
			if cr.Credentials.Username == "" && cr.Credentials.Password == "" {
				// We already tried with no username and password, return the error
				return nil, err
			}
			token, err = requestAuthToken(WwwAuthenticate, "", "")
			if err != nil {
				return nil, err
			}
		}
		fmt.Printf("Got token: %s\n", token)
		cr.TokenCv.L.Lock()
		cr.token = token
		cr.gotToken = true
		cr.waitingForToken = false
		cr.TokenCv.L.Unlock()
		goto retryRequest

	}
	return res, err
}
