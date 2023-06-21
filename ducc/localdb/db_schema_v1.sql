CREATE table "wishes" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT ,
    "cvmfsRepo" TEXT NOT NULL,
    "inputUri" TEXT NOT NULL,
    "outputUri" TEXT NOT NULL,
    "source" TEXT NOT NULL,

    "createLayers" INTEGER NOT NULL DEFAULT 0,
    "createThinImage" INTEGER NOT NULL DEFAULT 0,
    "createPodman" INTEGER NOT NULL DEFAULT 0,
    "createFlat" INTEGER NOT NULL DEFAULT 0,

    "webhookEnabled" INTEGER NOT NULL DEFAULT 0,
	"fullSyncIntervalSec" INTEGER NOT NULL DEFAULT 0,
	"lastConfigUpdate" TEXT NOT NULL DEFAULT "",
	"lastFullSync" TEXT NOT NULL DEFAULT ""
);

CREATE table "images" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT,
    "scheme" TEXT NOT NULL,
    "registry" TEXT NOT NULL,
    "repository" TEXT NOT NULL,
    "tag" TEXT,
    "digest" TEXT,

    "lastManifestCheck" TEXT,
    "lastChange" TEXT

    CHECK ("tag" IS NOT NULL OR "digest" IS NOT NULL)
);

CREATE table "wish_image" (
    "wishId" INTEGER NOT NULL,
    "imageId" INTEGER NOT NULL,

    FOREIGN KEY ("wishId") REFERENCES "wishes" ("id"),
    FOREIGN KEY ("imageId") REFERENCES "images" ("id")
);

CREATE table "layers" (
    "digest" TEXT PRIMARY KEY NOT NULL,
    "lastDownload" TEXT
);

create table "image_layer" (
    "imageId" INTEGER NOT NULL,
    "layerDigest" TEXT NOT NULL,

    FOREIGN KEY ("imageId") REFERENCES "images" ("id"),
    FOREIGN KEY ("layerDigest") REFERENCES "layers" ("digest")
);

PRAGMA user_version = 1;