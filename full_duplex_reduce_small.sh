BUILD_FOLDER=cmake-build-release

$BUILD_FOLDER/full_duplex_test 0 4 6553600 &
$BUILD_FOLDER/full_duplex_test 1 4 6553600 &
$BUILD_FOLDER/full_duplex_test 2 4 6553600 &
$BUILD_FOLDER/full_duplex_test 3 4 6553600 &
wait