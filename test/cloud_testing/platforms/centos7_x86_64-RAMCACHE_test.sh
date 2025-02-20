#!/bin/sh

export CVMFS_PLATFORM_NAME="centos7-x86_64_RAMCACHE"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/005-asetup                               \
                                 src/004-davinci                              \
                                 src/007-testjobs                             \
                                 src/011-rmemptyfilesrebuild                  \
                                 src/014-corrupt_lru                          \
                                 src/015-rebuild_on_crash                     \
                                 src/040-aliencache                           \
                                 src/041-rocache                              \
                                 src/042-cleanuppipes                         \
                                 src/044-unpinonmount                         \
                                 src/057-parallelmakecache                    \
                                 src/064-fsck                                 \
                                 src/068-rocache                              \
                                 src/070-tieredcache                          \
                                 src/081-shrinkwrap                           \
                                 src/082-shrinkwrap-cms                       \
                                 src/084-premounted                           \
                                 src/089-external_cache_plugin                \
                                 src/094-attachmount                          \
                                 src/097-statfs                               \
                                 --                                           \
                                 src/0*                                       \
                                 src/1*                                       \
                              || retval=1

echo "running CernVM-FS client migration test cases..."
CVMFS_TEST_CLASS_NAME=ClientMigrationTests                        \
./run.sh $MIGRATIONTEST_CLIENT_LOGFILE                            \
         -o ${MIGRATIONTEST_CLIENT_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
            migration_tests/0*                                    \
         || retval=1

exit $retval
