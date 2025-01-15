#include <cassert>
#include <iostream>
#include <span>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/tcp.h>
#include <unordered_map>
#include <cstring>
#include <vector>
#include <sstream>
#include <sys/fcntl.h>
#include <poll.h>

#define BASE_PORT 48148

void fill_constant(std::span<float> &span, const float value) {
    std::fill_n(span.data(), span.size(), value);
}

void pipelineReduce(const std::span<float> &array, int rank, int world_size,
                    const std::unordered_map</* rank */int, /* socket fd */int> &peer_tx_sockets,
                    const std::unordered_map</* rank */int, /* socket fd */int> &peer_rx_sockets);

int main(const int argc, char **argv) {
    int rank{};
    int world_size{};
    uint64_t num_elements{};

    // parse arguments
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <rank> <world_size> <num_elements>" << std::endl;
        return 1;
    }
    rank = std::stoi(argv[1]);
    world_size = std::stoi(argv[2]);
    num_elements = std::stoull(argv[3]);

    if (rank >= world_size) {
        std::cerr << "Rank must be less than world size" << std::endl;
        return 1;
    }

    std::cout << "Hello from rank " << rank << " of " << world_size << "!" << std::endl;

    // open listening socket for incoming connections
    const int listen_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_socket == -1) {
        std::cerr << "Failed to open listening socket" << std::endl;
        return 1;
    }

    // allow connection even when socket is in the TIME_WAIT state
    constexpr int enable = 1;
    if (setsockopt(listen_socket, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) == -1) {
        std::cerr << "Failed to set socket option" << std::endl;
        return 1;
    }

    // bind to port
    sockaddr_in listen_address{};
    listen_address.sin_family = AF_INET;
    listen_address.sin_addr.s_addr = INADDR_ANY;
    listen_address.sin_port = htons(BASE_PORT + rank);
    if (bind(listen_socket, reinterpret_cast<sockaddr *>(&listen_address), sizeof(listen_address)) == -1) {
        std::cerr << "Failed to bind to port" << std::endl;
        return 1;
    }

    if (listen(listen_socket, SOMAXCONN) == -1) {
        std::cerr << "Failed to listen on socket" << std::endl;
        return 1;
    }

    // NOTE: in this example, we are establishing p2p connections
    // to all peers for simplicity. In a real application, you would
    // only establish connections to the ranks that are neighbors in
    // the reduce topology.

    // establish all rx connections
    std::unordered_map</* rank */int, /* socket fd */int> peer_rx_sockets{};
    peer_rx_sockets.reserve(world_size);
    std::thread accept_thread([world_size, rank, listen_socket, &peer_rx_sockets] {
        // accept all incoming connections until all ranks are connected
        for (int i = 0; i < world_size; ++i) {
            if (i == rank) {
                continue;
            }

            sockaddr_in peer_address{};
            socklen_t peer_address_len = sizeof(peer_address);
            int rx_socket;
            while (true) {
                rx_socket = accept(listen_socket, reinterpret_cast<sockaddr *>(&peer_address), &peer_address_len);
                if (rx_socket == -1) {
                    std::cerr << "[Rank: " << rank << "] Failed to accept connection; retrying..." << std::endl;
                    continue;
                }
                break;
            }
            // receive the rank from the peer
            int peer_rank{};
            while (true) {
                if (recv(rx_socket, &peer_rank, sizeof(peer_rank), 0) == -1) {
                    std::cerr << "[Rank: " << rank << "] Failed to receive rank from peer; retrying..." << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }
                break;
            }
            peer_rank = ntohl(peer_rank);
            peer_rx_sockets[peer_rank] = rx_socket;
        }
    });

    std::vector<std::string> rank_ips{};
    rank_ips.reserve(world_size);

    if (const char *rank_ips_env = std::getenv("RANK_IPS"); rank_ips_env != nullptr) {
        std::istringstream rank_ips_stream(rank_ips_env);
        std::string ip;
        while (std::getline(rank_ips_stream, ip, ';')) {
            rank_ips.push_back(ip);
        }
    }

    // establish all tx connections
    std::unordered_map</* rank */int, /* socket fd */int> peer_tx_sockets{};
    peer_tx_sockets.reserve(world_size);
    for (int i = 0; i < world_size; ++i) {
        if (i == rank) {
            std::cout << "[Rank: " << rank << "] Skipping connection to self (rank=" << i << ")" << std::endl;
            continue;
        }

        std::cout << "[Rank: " << rank << "] Attempting to connect to peer rank=" << i << " on port=" << (BASE_PORT + i)
                << std::endl;

        int tx_socket;
        int retries = 0;
        while (true) {
            tx_socket = socket(AF_INET, SOCK_STREAM, 0);
            if (tx_socket == -1) {
                std::cerr << "[Rank: " << rank << "] Failed to open p2p TX socket: " << strerror(errno) << std::endl;
                return 1;
            }

            int peer_port = BASE_PORT + i;
            sockaddr_in peer_address{};
            peer_address.sin_family = AF_INET;

            if (rank_ips.empty())
                peer_address.sin_addr.s_addr = inet_addr("127.0.0.1");
            else
                peer_address.sin_addr.s_addr = inet_addr(rank_ips[i].c_str());

            peer_address.sin_port = htons(peer_port);

            if (connect(tx_socket, reinterpret_cast<sockaddr *>(&peer_address), sizeof(peer_address)) == -1) {
                std::cerr << "[Rank: " << rank << "] Failed to connect to peer rank=" << i << " on port " << peer_port
                        << ": " << strerror(errno) << ", retrying..." << std::endl;
                close(tx_socket); // Close the failed socket before retrying
                std::this_thread::sleep_for(std::chrono::seconds(1));
                retries++;
                if (constexpr int max_retries = 10; retries >= max_retries) {
                    std::cerr << "[Rank: " << rank << "] Exceeded maximum retries to connect to peer rank=" << i <<
                            std::endl;
                    return 1;
                }
                continue; // Retry with a new socket
            }
            std::cout << "[Rank: " << rank << "] Successfully connected to peer rank=" << i << std::endl;
            break; // Exit the retry loop on successful connection
        }

        // Send the rank to the peer
        const int network_rank = htonl(rank);
        if (ssize_t bytes_sent = send(tx_socket, &network_rank, sizeof(network_rank), 0); bytes_sent == -1) {
            std::cerr << "[Rank: " << rank << "] Failed to send rank to peer rank=" << i << ": " << strerror(errno) <<
                    std::endl;
            close(tx_socket);
            return 1;
        }
        std::cout << "[Rank: " << rank << "] Sent rank to peer rank=" << i << std::endl;

        peer_tx_sockets[i] = tx_socket;
    }
    std::cout << "[Rank: " << rank << "] P2P TX connections established!" << std::endl;

    accept_thread.join(); // wait for all rx connections to be established
    std::cout << "[Rank: " << rank << "] P2P RX connections established!" << std::endl;

    // set all sockets to non-blocking
    {
        for (const auto &[_, socket]: peer_rx_sockets) {
            if (fcntl(socket, F_SETFL, O_NONBLOCK) == -1) {
                std::cerr << "Failed to set socket to non-blocking" << std::endl;
                return 1;
            }
        }
        for (const auto &[_, socket]: peer_tx_sockets) {
            if (fcntl(socket, F_SETFL, O_NONBLOCK) == -1) {
                std::cerr << "Failed to set socket to non-blocking" << std::endl;
                return 1;
            }
        }
    }

    const std::unique_ptr<float[]> result_ptr(new float[num_elements]);
    std::span result(result_ptr.get(), num_elements);

    fill_constant(result, 1.0f);

    const auto start_time = std::chrono::system_clock::now();
    pipelineReduce(result, rank, world_size, peer_tx_sockets, peer_rx_sockets);
    const auto end_time = std::chrono::system_clock::now();

    // assert all values are world_size (each rank contributes 1.0)
    for (size_t i = 0; i < num_elements; ++i) {
        assert(result[i] == static_cast<float>(world_size));
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "Rank " << rank << " has completed the reduce phase in (ms): " << duration.count() <<
            std::endl;

    return 0;
}

void applyReduce(const std::span<float> &dst_buffer, const std::span<float> &src_buffer, const size_t start_offset,
                 const size_t end_offset) {
    assert(start_offset % sizeof(float) == 0);
    assert(end_offset % sizeof(float) == 0);

    const size_t start_idx = start_offset / sizeof(float);
    const size_t end_idx = end_offset / sizeof(float);

    assert(dst_buffer.size() == src_buffer.size());

    float *__restrict__ dst = dst_buffer.data() + start_idx;
    const float *__restrict__ src = src_buffer.data() + start_idx;

    const size_t num_elements = end_idx - start_idx;
    for (size_t i = 0; i < num_elements; ++i) {
        dst[i] += src[i];
    }
}

void pipelineReduce(const std::span<float> &array, const int rank, const int world_size,
                    const std::unordered_map</* rank */int, /* socket fd */int> &peer_tx_sockets,
                    const std::unordered_map</* rank */int, /* socket fd */int> &peer_rx_sockets) {
    assert(array.size() % world_size == 0); // for simplicity

    const size_t chunk_size = array.size() / world_size;

    const std::unique_ptr<float[]> recv_buffer(new float[chunk_size]);
    const std::span recv_span(recv_buffer.get(), chunk_size);

    const std::unique_ptr<float[]> orig_bak(new float[array.size()]);
    std::memcpy(orig_bak.get(), array.data(), array.size_bytes());
    const std::span orig_data(orig_bak.get(), array.size());


    for (int stage = 0; stage < world_size; stage++) {
        // each peer has one tx and rx peer in the ring
        // the rx peer is always the previous peer in the ring (with wrap-around)
        // the tx peer is always the next peer in the ring (with wrap-around)
        const int rx_peer = (rank - 1 + world_size) % world_size;
        const int tx_peer = (rank + 1) % world_size;

        const int rx_socket = peer_tx_sockets.at(rx_peer);
        const int tx_socket = peer_rx_sockets.at(tx_peer);

        // the tx chunk is the chunk we transmit
        // the rx chunk is the chunk we receive
        // they are different subsets of the array.
        // Sending and receiving occurs concurrently to utilize full duplex.
        // The tx & rx chunks is dependent on both rank and stage.
        // They are constructed such that each peer after world size stages has accumulated all data.
        const int rx_chunk = (world_size - stage - 1 + rank) % world_size;
        const int tx_chunk = (world_size - stage - 1 + rank + 1) % world_size;

        const std::span<float> tx_span = orig_data.subspan(tx_chunk * chunk_size, chunk_size);
        const std::span<float> rx_span = array.subspan(rx_chunk * chunk_size, chunk_size);

        // perform concurrent send and receive with fused reduce
        {
            size_t bytes_sent = 0;
            size_t bytes_recvd = 0;

            // We'll stage incoming data into recv_buffer, then copy to rx_span when complete
            const auto recv_ptr = reinterpret_cast<char *>(recv_span.data());
            const auto send_ptr = reinterpret_cast<const char *>(tx_span.data());

            while (bytes_sent < tx_span.size_bytes() || bytes_recvd < rx_span.size_bytes()) {
                pollfd fds[2];
                // We want to poll for "ready to write" on tx_socket only if there's data left to send
                fds[0].fd = tx_socket;
                fds[0].events = bytes_sent < tx_span.size_bytes() ? POLLOUT : 0;

                // We want to poll for "ready to read" on rx_socket only if we still expect more data
                fds[1].fd = rx_socket;
                fds[1].events = bytes_recvd < rx_span.size_bytes() ? POLLIN : 0;

                if (const int rc = poll(fds, 2, /* timeout */ -1); rc < 0) {
                    std::cerr << "poll() failed: " << strerror(errno) << std::endl;
                    return;
                }

                // If tx_socket is ready to write, send some data
                if ((fds[0].revents & POLLOUT) == POLLOUT) {
                    const ssize_t s = send(tx_socket, send_ptr + bytes_sent,
                                           tx_span.size_bytes() - bytes_sent, 0);
                    if (s < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                        std::cerr << "send() failed: " << strerror(errno) << std::endl;
                    } else if (s > 0) {
                        bytes_sent += s;
                    }
                }

                // If rx_socket is ready to read, read some data
                if ((fds[1].revents & POLLIN) == POLLIN) {
                    const ssize_t r = recv(rx_socket, recv_ptr + bytes_recvd,
                                           rx_span.size_bytes() - bytes_recvd, 0);
                    if (r <= 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                        std::cerr << "recv() failed: " << strerror(errno) << std::endl;
                        return;
                    }
                    if (r > 0) {
                        constexpr size_t el_size = sizeof(float);
                        const size_t old_bytes_recvd_ceiled = (bytes_recvd + el_size - 1) / el_size * el_size;
                        bytes_recvd += r;
                        const size_t bytes_recvd_floored = (bytes_recvd / el_size) * el_size;
                        assert(old_bytes_recvd_ceiled < bytes_recvd && bytes_recvd_floored <= bytes_recvd_floored);
                        applyReduce(rx_span, recv_span, old_bytes_recvd_ceiled, bytes_recvd_floored);
                    }
                }
            }
        }
    }
}
