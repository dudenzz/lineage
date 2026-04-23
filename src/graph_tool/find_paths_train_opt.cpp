#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <omp.h>
#include <chrono>
#include <atomic>
#include <iomanip>
#include <algorithm>
#include <random>

struct Triple { int s, p, o, neg; };
struct Edge { int to; int rel; };

class Graph {
public:
    // CSR Format for performance
    std::vector<int> head; 
    std::vector<Edge> edges;
    std::vector<Triple> triples;
    int max_node_id = 0;

    void load_from_file(const std::string& filename) {
        std::ifstream file(filename);
        int s, o, r, neg;
        std::vector<Triple> temp_triples;
        
        while (file >> neg >> s >> o >> r) {
            temp_triples.push_back({s, r, o, neg});
            if (neg == 1) {
                max_node_id = std::max({max_node_id, s, o});
            }
        }
        triples = temp_triples;

        // Build CSR structure
        std::vector<std::vector<Edge>> adj(max_node_id + 1);
        for (const auto& t : triples) {
            if (t.neg == 1) adj[t.s].push_back({t.o, t.p});
        }

        // Shuffle for diversity as per original logic
        std::random_device rd;
        std::mt19937 g(rd());
        for (auto& list : adj) std::shuffle(list.begin(), list.end(), g);

        head.resize(max_node_id + 2, 0);
        for (int i = 0; i <= max_node_id; ++i) {
            head[i + 1] = head[i] + adj[i].size();
            for (const auto& e : adj[i]) edges.push_back(e);
        }
    }

    void find_paths_recursive(int current, int target, int max_depth, int cap_branching,
                            std::vector<int>& visited, int query_id,
                            std::vector<int>& path_stack,
                            std::vector<std::vector<int>>& found_paths) {
        
        if (current == target) {
            if (path_stack.size() >= 2) {
                found_paths.push_back(path_stack);
            }
            return;
        }

        if (path_stack.size() >= (size_t)max_depth || found_paths.size() >= 3) return;

        int count = 0;
        for (int i = head[current]; i < head[current + 1]; ++i) {
            if (count++ >= cap_branching) break;
            const Edge& e = edges[i];

            if (visited[e.to] != query_id) {
                visited[e.to] = query_id;
                path_stack.push_back(e.rel);
                find_paths_recursive(e.to, target, max_depth, cap_branching, visited, query_id, path_stack, found_paths);
                path_stack.pop_back();
                visited[e.to] = 0; // Backtrack

                if (found_paths.size() >= 3) break;
            }
        }
    }
};

int main(int argc, char* argv[]) {
    int max_depth = 7, base_branch = 20, cap_branch = 200;
    if (argc >= 4) {
        max_depth = std::stoi(argv[1]);
        base_branch = std::stoi(argv[2]); // Kept for CLI compatibility
        cap_branch = std::stoi(argv[3]);
    }

    Graph g;
    g.load_from_file("triples.data");
    
    std::ofstream outfile("paths_output.txt");
    std::atomic<long> processed(0);
    auto start_time = std::chrono::steady_clock::now();

    #pragma omp parallel
    {
        // Use int vector for timestamp-based visited check
        std::vector<int> visited(g.max_node_id + 1, 0);
        int query_id = 0; 
        std::vector<int> path_stack;
        std::vector<std::vector<int>> local_found;
        std::stringstream ss;

        #pragma omp for schedule(dynamic, 64)
        for (int i = 0; i < (int)g.triples.size(); ++i) {
            const Triple& t = g.triples[i];
            local_found.clear();
            path_stack.clear();
            
            // Increment query_id instead of clearing the whole visited vector
            query_id++; 
            visited[t.s] = query_id;
            
            // Run DFS once with max branching factor
            g.find_paths_recursive(t.s, t.o, max_depth, cap_branch, visited, query_id, path_stack, local_found);

            for (const auto& path : local_found) {
                ss << t.neg << " " << t.s << " " << t.p << " " << t.o << " :";
                for (size_t j = 0; j < path.size(); ++j) {
                    ss << (j == 0 ? "" : " ") << path[j];
                }
                ss << "\n";
            }

            // Larger buffer before locking
            if (ss.tellp() > 2 * 1024 * 1024) {
                #pragma omp critical
                { outfile << ss.rdbuf(); }
                ss.str(""); ss.clear();
            }

            if (omp_get_thread_num() == 0 && ++processed % 1000 == 0) {
                double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start_time).count();
                std::cout << "\rProgress: " << std::fixed << std::setprecision(2) 
                          << (processed * 100.0 / g.triples.size()) << "% | Speed: " 
                          << (int)(processed / (elapsed + 1e-6)) << " n/s" << std::flush;
            }
        }
        #pragma omp critical
        { outfile << ss.rdbuf(); }
    }

    std::cout << "\nTotal Time: " << std::chrono::duration<double>(std::chrono::steady_clock::now() - start_time).count() << "s\n";
    return 0;
}