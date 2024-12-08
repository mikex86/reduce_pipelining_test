sudo apt install -y cmake make gcc g++ wget
wget https://raw.githubusercontent.com/mikex86/reduce_pipelining_test/refs/heads/master/CMakeLists.txt
wget https://raw.githubusercontent.com/mikex86/reduce_pipelining_test/refs/heads/master/half_duplex.cpp
wget https://raw.githubusercontent.com/mikex86/reduce_pipelining_test/refs/heads/master/full_duplex.cpp
mkdir cmake-build-release
cd cmake-build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j

# wget https://raw.githubusercontent.com/mikex86/reduce_pipelining_test/refs/heads/master/half_duplex_reduce_gcloud.sh
# wget https://raw.githubusercontent.com/mikex86/reduce_pipelining_test/refs/heads/master/full_duplex_reduce_gcloud.sh
