BUILD_FOLDER=cmake-build-release

export RANK_IPS="127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1;127.0.0.1"

$BUILD_FOLDER/full_duplex_test 0 16 65536000 &
$BUILD_FOLDER/full_duplex_test 1 16 65536000 &
$BUILD_FOLDER/full_duplex_test 2 16 65536000 &
$BUILD_FOLDER/full_duplex_test 3 16 65536000 &
$BUILD_FOLDER/full_duplex_test 4 16 65536000 &
$BUILD_FOLDER/full_duplex_test 5 16 65536000 &
$BUILD_FOLDER/full_duplex_test 6 16 65536000 &
$BUILD_FOLDER/full_duplex_test 7 16 65536000 &
$BUILD_FOLDER/full_duplex_test 8 16 65536000 &
$BUILD_FOLDER/full_duplex_test 9 16 65536000 &
$BUILD_FOLDER/full_duplex_test 10 16 65536000 &
$BUILD_FOLDER/full_duplex_test 11 16 65536000 &
$BUILD_FOLDER/full_duplex_test 12 16 65536000 &
$BUILD_FOLDER/full_duplex_test 13 16 65536000 &
$BUILD_FOLDER/full_duplex_test 14 16 65536000 &
$BUILD_FOLDER/full_duplex_test 15 16 65536000 &

wait