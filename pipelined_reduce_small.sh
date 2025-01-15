BUILD_FOLDER=cmake-build-release

export RANK_IPS="127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1"

$BUILD_FOLDER/pipelined_test 0 4 6553600 &
$BUILD_FOLDER/pipelined_test 1 4 6553600 &
$BUILD_FOLDER/pipelined_test 2 4 6553600 &
$BUILD_FOLDER/pipelined_test 3 4 6553600 &
wait