BUILD_FOLDER=cmake-build-release

$BUILD_FOLDER/reduce_pipelining_test 0 4 10000000 &
$BUILD_FOLDER/reduce_pipelining_test 1 4 10000000 &
$BUILD_FOLDER/reduce_pipelining_test 2 4 10000000 &
$BUILD_FOLDER/reduce_pipelining_test 3 4 10000000 &
wait