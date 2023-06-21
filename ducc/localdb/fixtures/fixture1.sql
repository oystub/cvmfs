-- Basic fixture containing:
--  - 2 wishes

INSERT INTO "wishes" 
    (   "id", 
        "cvmfsRepo", 
        "inputUri", 
        "outputUri", 
        "source", 
        "createLayers", 
        "createThinImage", 
        "createPodman", 
        "createFlat", 
        "webhookEnabled", 
        "fullSyncIntervalSec", 
        "lastConfigUpdate", 
        "lastFullSync"
    )
VALUES 
    (   1, 
        "repo.invalid", 
        "https://registry.invalid/repo:tag", 
        "https://another.registry.invalid/repo:tag", 
        "testfixture", 
        1, 
        1, 
        1, 
        1, 
        1, 
        1234, 
        "2022-01-01 00:00:00.000", 
        "2022-01-01 00:00:00.000"
    );

INSERT INTO "wishes"
    (   "id", 
        "cvmfsRepo", 
        "inputUri", 
        "outputUri", 
        "source", 
        "createLayers", 
        "createThinImage", 
        "createPodman", 
        "createFlat", 
        "webhookEnabled", 
        "fullSyncIntervalSec", 
        "lastConfigUpdate", 
        "lastFullSync"
    )
VALUES 
    (   2, 
        "repo2.invalid", 
        "https://registry2.invalid/repo:tag", 
        "https://another.registry2.invalid/repo:tag", 
        "testfixture", 
        0, 
        0, 
        0, 
        0, 
        0, 
        0, 
        "", 
        ""
    );