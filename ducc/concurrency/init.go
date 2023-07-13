package concurrency

func init() {
	initLayerTarFileCache("/tmp/ducc/.layertars")
	initLayerCache("/tmp/ducc/.layers", "local.test.repo")
}
