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
    std::vector<std::vector<Edge>> adj; 
    std::vector<Triple> triples;
    int max_node_id = 0;

    void load_from_file(const std::string& filename) {
        std::ifstream file(filename);
        int s, o, r, neg;
        while (file >> neg >> s >> o >> r) {
            if(neg == 1) {
                if (s > max_node_id) max_node_id = s;
                if (o > max_node_id) max_node_id = o;
                if ((int)adj.size() <= std::max(s, o)) adj.resize(std::max(s, o) + 1);
                adj[s].push_back({o, r});
            }
            triples.push_back({s, r, o, neg});
        }
        std::random_device rd;
        std::mt19937 g(rd());
        for (auto& list : adj) std::shuffle(list.begin(), list.end(), g);
    }

    void find_paths_recursive(int current, int target, int max_depth, int current_branching,
                            std::vector<uint8_t>& visited, 
                            std::vector<int>& current_rel_path,
                            std::vector<std::vector<int>>& found_paths) {
        
        if (current == target) {
            if (current_rel_path.size() >= 2) {
                bool exists = false;
                for(const auto& p : found_paths) if(p == current_rel_path) { exists = true; break; }
                if(!exists) found_paths.push_back(current_rel_path);
            }
            return;
        }

        if (current_rel_path.size() >= (size_t)max_depth || found_paths.size() >= 3) return;

        int branches_explored = 0;
        for (const auto& edge : adj[current]) {
            if (branches_explored >= current_branching) break; 

            if (!visited[edge.to]) {
                visited[edge.to] = 1;
                current_rel_path.push_back(edge.rel);
                find_paths_recursive(edge.to, target, max_depth, current_branching, visited, current_rel_path, found_paths);
                current_rel_path.pop_back();
                visited[edge.to] = 0;
                
                branches_explored++;
                if (found_paths.size() >= 3) break;
            }
        }
    }
};

int main(int argc, char* argv[]) {
    // CLI Arguments: depth, base_branch, cap_branch
    int max_depth = 7;
    int base_branch = 20;
    int cap_branch = 200;

    if (argc >= 4) {
        max_depth = std::stoi(argv[1]);
        base_branch = std::stoi(argv[2]);
        cap_branch = std::stoi(argv[3]);
    } else {
        std::cout << "Usage: ./pathfinder [max_depth] [base_branching] [cap_branching]\n"
                  << "Using defaults: Depth=" << max_depth << ", Base=" << base_branch << ", Cap=" << cap_branch << "\n\n";
    }

    Graph g;
    g.load_from_file("triples.data");
    
    std::ofstream outfile("paths_output.txt");
    std::atomic<long> processed(0);
    int total = g.triples.size();
    auto start_time = std::chrono::steady_clock::now();
    
    #pragma omp parallel
    {   
        std::vector<uint8_t> visited(g.max_node_id + 1, 0);
        std::vector<int> path_stack;
        std::vector<std::vector<int>> local_found;
        std::stringstream ss;
        
        #pragma omp for schedule(dynamic, 100)
        for (int i = 0; i < total; ++i) {
            const Triple& t = g.triples[i];
            local_found.clear();
            
            // Adaptive Loop: Start narrow, expand only if needed
            int current_b = base_branch;
            while(local_found.size() < 3 && current_b <= cap_branch) {
                path_stack.clear();
                visited[t.s] = 1;
                g.find_paths_recursive(t.s, t.o, max_depth, current_b, visited, path_stack, local_found);
                visited[t.s] = 0;
                
                if(local_found.size() < 3) current_b *= 2; 
                else break;
            }

            for (const auto& path : local_found) {
                ss << t.neg << " " << t.s << " "  <<t.p << " " << t.o<<" " << ":"; 
                for (size_t j = 0; j < path.size(); ++j) ss << path[j] << (j == path.size() - 1 ? "" : " ");
                ss << "\n";
            }

            if (ss.tellp() > 1024 * 1024) {
                #pragma omp critical
                { outfile << ss.rdbuf(); outfile.flush(); }
                ss.str(""); ss.clear();
            }

            long current_val = ++processed;
            if (omp_get_thread_num() == 0 && current_val % 500 == 0) {
                double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start_time).count();
                double speed = current_val / (elapsed + 1e-6);
                std::cout << "\rProgress: " << std::fixed << std::setprecision(2) << (current_val * 100.0 / total) << "% | Speed: " << (int)speed << " n/s" << std::flush;
            }
        }
        #pragma omp critical
        { outfile << ss.rdbuf(); }
    }
    outfile.close();
    std::cout << "\nDone. Elapsed: " << std::chrono::duration<double>(std::chrono::steady_clock::now() - start_time).count() << "s\n";
    return 0;
}