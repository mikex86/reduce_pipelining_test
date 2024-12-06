#include <cassert>
#include <iostream>
#include <span>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#define BASE_PORT 48148

void fill_constant(std::span<float> span, const float value) {
    for (float &elem: span) {
        elem = value;
    }
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
        constexpr int max_retries = 10;
        while (true) {
            tx_socket = socket(AF_INET, SOCK_STREAM, 0);
            if (tx_socket == -1) {
                std::cerr << "[Rank: " << rank << "] Failed to open p2p TX socket: " << strerror(errno) << std::endl;
                return 1;
            }

            int peer_port = BASE_PORT + i;
            sockaddr_in peer_address{};
            peer_address.sin_family = AF_INET;
            peer_address.sin_addr.s_addr = inet_addr("127.0.0.1");
            peer_address.sin_port = htons(peer_port);

            if (connect(tx_socket, reinterpret_cast<sockaddr *>(&peer_address), sizeof(peer_address)) == -1) {
                std::cerr << "[Rank: " << rank << "] Failed to connect to peer rank=" << i << " on port " << peer_port
                        << ": " << strerror(errno) << ", retrying..." << std::endl;
                close(tx_socket); // Close the failed socket before retrying
                std::this_thread::sleep_for(std::chrono::seconds(1));
                retries++;
                if (retries >= max_retries) {
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
    const std::span result(result_ptr.get(), num_elements);

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

    uint64_t total_time = 0;

    if (!should_initiate_ring && prev_rank >= 0) {
        const int prev_rx_socket = peer_rx_sockets[prev_rank];
        const std::unique_ptr<float[]> recv_buffer_ptr(new float[num_elements]);
        const std::span recv_buffer(recv_buffer_ptr.get(), num_elements);

        const auto start = std::chrono::high_resolution_clock::now();

        // receive data from previous rank
        size_t bytes_received = 0;
        while (bytes_received < recv_buffer.size_bytes()) {
            const size_t bytes_remaining = recv_buffer.size_bytes() - bytes_received;
            const size_t bytes_received_now = recv(prev_rx_socket,
                                                   reinterpret_cast<uint8_t *>(recv_buffer.data()) + bytes_received,
                                                   bytes_remaining, 0);
            if (bytes_received_now == -1) {
                std::cerr << "[Rank: " << rank << "] Failed to receive data from previous rank" << std::endl;
                return 1;
            }
            bytes_received += bytes_received_now;
        }

        const auto end = std::chrono::high_resolution_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        total_time += duration;

        // receive cumulative time from previous rank
        uint64_t cumulative_time_network;
        if (recv(prev_rx_socket, &cumulative_time_network, sizeof(cumulative_time_network), 0) == -1) {
            std::cerr << "[Rank: " << rank << "] Failed to receive cumulative time from previous rank" << std::endl;
            return 1;
        }
        const uint64_t cumulative_time = ntohll(cumulative_time_network);
        total_time += cumulative_time;

        // shutdown write side of rx socket
        if (shutdown(prev_rx_socket, SHUT_WR) == -1) {
            std::cerr << "[Rank: " << rank << "] Failed to shutdown write side of rx socket" << std::endl;
            return 1;
        }
        // drain the socket to ensure complete transmission to the next rank (yes, this is necessary because tcp is weird)
        uint8_t dummy{};
        while (recv(prev_rx_socket, &dummy, sizeof(dummy), 0) > 0) {
        }

        // perform reduction
        for (size_t i = 0; i < num_elements; ++i) {
            result[i] += recv_buffer[i];
        }
    }
    if (next_rank < world_size) {
        // send data to next rank
        size_t bytes_sent = 0;
        while (bytes_sent < result.size_bytes()) {
            const size_t bytes_remaining = result.size_bytes() - bytes_sent;
            const size_t bytes_sent_now = write(next_tx_socket, result.data() + bytes_sent, bytes_remaining);
            if (bytes_sent_now == -1) {
                // get reason for send failure

                return 1;
            }
            bytes_sent += bytes_sent_now;
        }
        // send cumulative time to next rank
        const uint64_t cumulative_time_network = htonll(total_time);
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
    std::cout << "[Rank: " << rank << "] Total reduce time after rank " << rank << ": " << (
        static_cast<double>(total_time) / 1'000'000.0) << " ms" << std::endl;
    if (next_rank == world_size) {
        // the reduce phase of the ring reduce is complete
        // we only simulate the reduce phase here because it is
        // the most reliant on pipelining
        std::cout << "Rank " << rank << " has completed the reduce phase." << std::endl;
    }
    return 0;
}