#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <omp.h>
#include <chrono>
#include <atomic>
#include <iomanip>
#include <algorithm>
#include <set>
#include <algorithm>
#include <random>
struct Path {
    std::vector<int> nodes;
};
struct Triple {
    int s, p, o, neg;
};
class Graph {
public:
    // We use a vector of ints; we don't need the 'rel' ID for pathfinding nodes
    std::vector<std::vector<int>> adj; 
    std::vector<Triple> triples;
    int max_node_id = 0;

    void load_from_file(const std::string& filename) {
        std::ifstream file(filename);
        int s, o, r, neg;
        struct RawEdge { int s, p, o; };
        std::vector<RawEdge> temp;

        while (file >> neg >> s >> o >> r) {
            if(neg == 1)
            {
                temp.push_back({s, r, o});
                if (s > max_node_id) max_node_id = s;
                if (o > max_node_id) max_node_id = o;
            }
            triples.push_back({s,r,o,neg});
        }

        adj.resize(max_node_id + 1);
        for (auto& edge : temp) {
            adj[edge.s].push_back(edge.o);
        }

        // Remove duplicate edges (s -> o) so we don't search twice
        for (int i = 0; i <= max_node_id; ++i) {
            std::sort(adj[i].begin(), adj[i].end());
            adj[i].erase(std::unique(adj[i].begin(), adj[i].end()), adj[i].end());
        }
        //shuffle all adjececy lists, so we get a different result every execution (this is a simulation of drawing a random path, altough longer paths will be drawn more often)
        std::random_device rd;
        std::mt19937 g(rd());
        for (int i = 0; i <= max_node_id; ++i) {
            std::shuffle(adj[i].begin(), adj[i].end(), g); }
    }

    void find_paths_recursive(int current, int target, int max_depth, 
                            std::vector<uint8_t>& visited, 
                            std::vector<int>& current_path,
                            std::vector<std::vector<int>>& found_for_this_pair) {
        if (current == target) {
            if (current_path.size() >= 3) {
                // Manual uniqueness check (faster than set for small N)
                for(const auto& p : found_for_this_pair) if(p == current_path) return;
                found_for_this_pair.push_back(current_path);
            }
            return;
        }

        if (current_path.size() >= max_depth || found_for_this_pair.size() >= 3) return;

        for (int neighbor : adj[current]) {
            if (!visited[neighbor]) {
                visited[neighbor] = 1;
                current_path.push_back(neighbor);
                find_paths_recursive(neighbor, target, max_depth, visited, current_path, found_for_this_pair);
                current_path.pop_back();
                visited[neighbor] = 0;
                if (found_for_this_pair.size() >= 3) break;
            }
        }
    }
};

int main() {
    std::cout << "Creating graph structure...\n";
    Graph g;
    std::cout << "Reading graph data.\n";
    g.load_from_file("triples.data");
    std::cout << "Graph loaded.\n";
    std::ofstream outfile("paths_output.txt");
    std::atomic<long> processed_nodes(0);
    int total_nodes = g.triples.size();
    auto start_time = std::chrono::steady_clock::now();
    
    std::cout << "Starting parallel execution.\n";
    #pragma omp parallel
    {   
        // #pragma omp critical
        // {
        //     std::cout << "Thread " << omp_get_thread_num()<<" started.\n";
        // }
        std::vector<uint8_t> visited(g.max_node_id + 1, 0);
        std::vector<int> path_stack;
        std::stringstream ss;
        
        #pragma omp for schedule(dynamic, 10)
        for (Triple t : g.triples) {



            
            // Use a set to ensure unique paths for this specific S-T pair
            std::vector<std::vector<int>> found_for_this_pair;
            
            visited.clear();
            path_stack.clear();
            visited.push_back(t.s);
            path_stack.push_back(t.s);

            g.find_paths_recursive(t.s, t.o, 10, visited, path_stack, found_for_this_pair);

            // Write relation id to the buffer
            // Write the unique paths to the buffer
            
            for (const auto& path : found_for_this_pair) {
                ss << t.p << " " << t.neg << ":"; 
                for (size_t i = 0; i < path.size(); ++i) {
                    ss << path[i] << (i == path.size() - 1 ? "" : " ");
                }
                ss << "\n";
            }
            

            // Thread-safe progress and periodic flush
            if (ss.tellp() > 512000) { // 512KB buffer
                #pragma omp critical
                {
                    outfile << ss.rdbuf();
                    outfile.flush();
                }
                ss.str("");
                ss.clear();
            }
            long current_val = ++processed_nodes;
            if (omp_get_thread_num() == 0) {
                auto now = std::chrono::steady_clock::now();
                std::chrono::duration<double> elapsed = now - start_time;
                
                double percent = (double)current_val / total_nodes * 100.0;
                double speed = current_val / (elapsed.count() + 0.001);
                double remaining_sec = (total_nodes - current_val) / (speed + 0.001);

                std::cout << "\rProgress: " << std::fixed << std::setprecision(2) << percent << "% "
                          << "| Nodes/s: " << (int)speed 
                          << "| ETA: " << (int)remaining_sec / 60 << "m " << (int)remaining_sec % 60 << "s    " 
                          << std::flush;
            }
        }

        #pragma omp critical
        {
            outfile << ss.rdbuf();
        }
    }

    std::cout << "\nDone! Unique paths saved." << std::endl;
    return 0;
}