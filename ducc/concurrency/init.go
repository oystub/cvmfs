package concurrency

func init() {
	initLayerTarFileCache("/tmp/ducc/.layertars")
	initLayerIngest()
	initIngestChain()
}
