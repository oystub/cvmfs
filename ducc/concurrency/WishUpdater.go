package concurrency

import (
	"context"
	"fmt"
	"sync"

	"github.com/cvmfs/ducc/lib"
)

func UpdateWishes(ctx context.Context, op GenericOperation, wishes []lib.WishInternal) {
	//tarFileDownloader := NewTarFileDownloader("/tmp/ducc/tarfiles")
	//chainIngester := NewChainIngester(tarFileDownloader)

	var operations = make([]GenericOperation, len(wishes))

	// Create an operation for each wish
	for i, wish := range wishes {
		ctx, cancel := context.WithCancel(ctx)

		updateWishOperation := InputOutputOperation[lib.WishInternal, any]{
			Ctx:       ctx,
			CtxCancel: cancel,
			Status:    NewStatusHandle(TS_NotStarted),
			Input:     wish,
			statusCv:  sync.Cond{L: &sync.Mutex{}},
		}
		updateWishOperation.Status.Name = "Expand wildcards for " + wish.WishIdentifier.String()
		updateWishOperation.Status.OperationType = "UpdateWish"
		updateWishOperation.Status.ParentHandle = op.StatusHandle()
		op.StatusHandle().addChildHandle(updateWishOperation.Status)

		expandWildCardOp := ExpandWildcards(op, wish)
		operations[i] = expandWildCardOp
	}

	// Expand wildcards for each wish
	fmt.Print("Expanding wildcards for wishes: ")
	for _, op := range operations {
		op := op
		go func() {
			for range op.Done() {
			}
			fmt.Printf("Done with %s\n", op.StatusHandle().Name)
		}()
	}

	for _, op := range operations {
		<-op.Done()
		fmt.Println("Done")
		op.StatusHandle().printStatus()
	}

}

func ExpandWildcards(parentOp GenericOperation, wish lib.WishInternal) *InputOutputOperation[lib.WishInternal, lib.WishInternalWithTags] {
	ctx, cancelCtx := context.WithCancel(context.Background())
	status := NewStatusHandle(TS_NotStarted)
	status.Name = "Expand wildcards for " + wish.WishIdentifier.String()
	status.OperationType = "ExpandWildcards"
	status.ParentHandle = parentOp.StatusHandle()
	parentOp.StatusHandle().addChildHandle(status)

	op := InputOutputOperation[lib.WishInternal, lib.WishInternalWithTags]{
		Ctx:       ctx,
		CtxCancel: cancelCtx,
		Status:    status,
		Input:     wish,
		statusCv:  sync.Cond{L: &sync.Mutex{}},
	}

	if wish.WishIdentifier.ImageDigest != "" {
		op.Output = lib.WishInternalWithTags{
			WishInternal: wish,
			Tags: []*lib.Tag{
				{
					Repository: wish.Repository,
					Name:       wish.WishIdentifier.ImageDigest.String(),
				},
			},
		}
		op.Status.SetStatus(TS_Success)
		op.statusCv.Broadcast()
		return &op
	}

	go func() {
		defer op.statusCv.Broadcast()
		// TODO: Cancellable context
		tagStr := wish.WishIdentifier.TagStr
		tags, err := op.Input.Repository.FetchTags(tagStr)
		if err != nil {
			op.Status.SetStatus(TS_Failure)
			op.SetError(err)
			return
		}
		if len(tags) == 0 {
			op.Status.SetStatus(TS_Failure)
			op.SetError(fmt.Errorf("no matching tags!"))
			return
		}
		op.Output = lib.WishInternalWithTags{
			WishInternal: wish,
			Tags:         tags,
		}
		op.Status.SetStatus(TS_Success)
	}()
	return &op
}

func FetchManifest(parentOp GenericOperation, tag lib.Tag) *InputOutputOperation[lib.Tag, lib.TagWithManifest] {
	//ctx, cancelCtx := context.WithCancel(context.Background())
	status := NewStatusHandle(TS_NotStarted)
	status.Name = "Fetch manifest for " + tag.Name
	status.OperationType = "FetchManifest"
	status.ParentHandle = parentOp.StatusHandle()
	parentOp.StatusHandle().addChildHandle(status)

	/*op := InputOutputOperation[lib.Tag, lib.TagWithManifest]{
	Ctx:       ctx,
	CtxCancel: cancelCtx,
	Status:    status,
	Input:     tag,
	statusCv:  sync.Cond{L: &sync.Mutex{}},
	*/
	return nil
}
