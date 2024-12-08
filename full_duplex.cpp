#include <cassert>
#include <iostream>
#include <span>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <unordered_map>
#include <cstring>

#define BASE_PORT 48148

void fill_constant(std::span<float> &span, const float value) {
    std::fill_n(span.data(), span.size(), value);
}

bool set_non_blocking(const int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1) return false;
    flags |= O_NONBLOCK;
    return (fcntl(socket_fd, F_SETFL, flags) != -1);
}

bool set_blocking(const int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1) return false;
    flags &= ~O_NONBLOCK;
    return (fcntl(socket_fd, F_SETFL, flags) != -1);
}

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

    const std::unique_ptr<float[]> result_ptr(new float[num_elements]);
    std::span result(result_ptr.get(), num_elements);

    fill_constant(result, 1.0f);

    int next_rank;
    int prev_rank;
    bool should_initiate_ring = false;
    if (rank == 0) {
        // rank 0 initiates the reduce operation
        next_rank = 1;
        prev_rank = -1;
        should_initiate_ring = true;
    } else {
        next_rank = rank + 1;
        prev_rank = rank - 1;
    }

    const int next_tx_socket = peer_tx_sockets[next_rank];

    uint64_t cumulative_recv_time = 0;

    // perform main transmit loop
    {
        const int prev_rx_socket = peer_rx_sockets[prev_rank];

        const std::unique_ptr<float[]> recv_buffer_ptr(new float[num_elements]);
        const std::span recv_buffer(recv_buffer_ptr.get(), num_elements);

        constexpr size_t max_buffer_size = (1 << 20) * 10; // 10 MB

        uint64_t total_time_read_ns = 0;

        // receive, reduce and send data
        size_t total_bytes_processed = 0;
        while (total_bytes_processed < recv_buffer.size_bytes()) {
            if (!should_initiate_ring) {
                assert(prev_rank >= 0);

                size_t bytes_remaining = recv_buffer.size_bytes() - total_bytes_processed;
                size_t to_read = std::min(bytes_remaining, max_buffer_size);

                fd_set read_fds;
                FD_ZERO(&read_fds);
                FD_SET(prev_rx_socket, &read_fds);

                // Optional: Set a timeout for select
                timeval timeout{};
                timeout.tv_sec = 5; // 5 seconds timeout
                timeout.tv_usec = 0;

                if (int ready = select(prev_rx_socket + 1, &read_fds, nullptr, nullptr, &timeout); ready == -1) {
                    std::cerr << "[Rank: " << rank << "] select() failed: " << strerror(errno) << std::endl;
                    return 1;
                } else if (ready == 0) {
                    // timed out, just retry...
                    continue;
                }

                // Data is ready to be read
                auto recv_start = std::chrono::high_resolution_clock::now();
                ssize_t bytes_received_now = recv(prev_rx_socket,
                                                  reinterpret_cast<uint8_t *>(recv_buffer.data()) +
                                                  total_bytes_processed,
                                                  to_read, 0);
                auto recv_end = std::chrono::high_resolution_clock::now();

                if (bytes_received_now == -1) {
                    if (errno == EWOULDBLOCK || errno == EAGAIN) {
                        // No data available, continue
                        continue;
                    }
                    std::cerr << "[Rank: " << rank << "] Failed to receive data from previous rank: "
                            << strerror(errno) << std::endl;
                    return 1;
                }
                if (bytes_received_now == 0) {
                    // Connection closed
                    break;
                }

                // Measure only the time spent in recv()
                uint64_t recv_time_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(recv_end - recv_start).
                        count();
                total_time_read_ns += recv_time_ns;

                if (next_rank < world_size) {
                    // sending the bytes we just received

                    // TODO: ACTUALLY APPLY REDUCE

                    size_t bytes_sent_current = 0;
                    while (bytes_sent_current < bytes_received_now) {
                        const size_t tx_bytes_remaining = bytes_received_now - bytes_sent_current;
                        const size_t to_send = tx_bytes_remaining < max_buffer_size
                                                   ? tx_bytes_remaining
                                                   : max_buffer_size;
                        const size_t bytes_sent_now = send(next_tx_socket,
                                                           reinterpret_cast<uint8_t *>(result.data()) +
                                                           total_bytes_processed + bytes_sent_current,
                                                           to_send, 0);
                        if (bytes_sent_now == -1) {
                            return 1;
                        }
                        bytes_sent_current += bytes_sent_now;
                    }
                    assert(bytes_sent_current == bytes_received_now);
                }
                total_bytes_processed += bytes_received_now;
            } else if (next_rank < world_size) {
                // send as much data as possible
                const size_t bytes_remaining = recv_buffer.size_bytes() - total_bytes_processed;
                const size_t to_send = bytes_remaining < max_buffer_size ? bytes_remaining : max_buffer_size;
                const size_t bytes_sent_now = send(next_tx_socket,
                                                   reinterpret_cast<uint8_t *>(recv_buffer.data()) +
                                                   total_bytes_processed, to_send, 0);
                if (bytes_sent_now == -1) {
                    std::cerr << "[Rank: " << rank << "] Failed to send data to next rank: " << strerror(errno) <<
                            std::endl;
                    return 1;
                }
                total_bytes_processed += bytes_sent_now;
            }
        }
        cumulative_recv_time += total_time_read_ns;

        // make prev rx socket blocking again
        set_blocking(prev_rx_socket);

        if (prev_rank >= 0) {
            // receive cumulative time from previous rank
            uint64_t cumulative_time_network;
            if (recv(prev_rx_socket, &cumulative_time_network, sizeof(cumulative_time_network), 0) == -1) {
                std::cerr << "[Rank: " << rank << "] Failed to receive cumulative time from previous rank" << std::endl;
                return 1;
            }
            const uint64_t cumulative_time = ntohl(cumulative_time_network);
            cumulative_recv_time += cumulative_time;
        }
    }
    if (next_rank < world_size) {
        // send cumulative time to next rank
        const uint64_t cumulative_time_network = htonl(cumulative_recv_time);
        if (send(next_tx_socket, &cumulative_time_network, sizeof(cumulative_time_network), 0) == -1) {
            std::cerr << "[Rank: " << rank << "] Failed to send cumulative time to next rank" << std::endl;
            return 1;
        }
        // shutdown write side of tx socket
        if (shutdown(next_tx_socket, SHUT_WR) == -1) {
            std::cerr << "[Rank: " << rank << "] Failed to shutdown write side of tx socket" << std::endl;
            return 1;
        }
        // drain the socket to ensure complete transmission to the next rank (yes, this is necessary because tcp is weird)
        uint8_t dummy{};
        while (recv(next_tx_socket, &dummy, sizeof(dummy), 0) > 0) {
        }
    }
    std::cout << "[Rank: " << rank << "] Total recv time after rank " << rank << ": " << (
        static_cast<double>(cumulative_recv_time) / 1'000'000.0) << " ms" << std::endl;
    if (next_rank == world_size) {
        // the reduce phase of the ring reduce is complete
        // we only simulate the reduce phase here because it is
        // the most reliant on pipelining
        std::cout << "Rank " << rank << " has completed the reduce phase." << std::endl;
    }
    return 0;
}
