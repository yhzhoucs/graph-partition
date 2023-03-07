#include <iostream>
#include <fstream>
#include <cinttypes>
#include <random>
#include <omp.h>

#include "BFS.h"
#include "Partition.h"
#include "ParQ.h"
#include "scheduler/Scheduler.h"
#include "scheduler/CommonScheduler.h"
#include "scheduler/PriorityScheduler.h"

using node_t = int32_t;
using partition_t = int8_t;
using depth_t = int32_t;

int main(int, char**) {
    omp_set_num_threads(30); // limit thread number to 30

    std::ifstream infile;
    infile.open("dataset/soc-Slashdot0811_p.txt", std::ios::in);
    if (!infile.is_open()) {
        std::cerr << "READ ERROR" << std::endl;
        return -1;
    }
    my::CSR<node_t> csr(infile);
    infile.close();
    std::cout << "READ GRAPH DONE" << std::endl;

    infile.open("dataset/soc-Slashdot0811-partition8", std::ios::in);
    if (!infile.is_open()) {
        std::cerr << "READ ERROR" << std::endl;
        return -1;
    }
    my::Partition<node_t, partition_t> partition(csr);
    partition.load_partition(infile);
    infile.close();
    std::cout << "READ PARTITION DONE" << std::endl;
    my::ParQ<node_t, partition_t> parq(csr, partition);
    parq.calculate_scores();
    std::cout << "GENERATE SCORES DONE" << std::endl;

    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, csr.get_vertex_number()-1);

    double avg_gain{0.0};
    int positive{0};
    int equal{0};
    int total{64};
    double avg_switches = 0.0;

    std::vector<node_t> sources;
    do {
        int s = dist(rng);
        if (csr.get_outdegree(s) >= 4) {
            sources.emplace_back(s);
        }
    } while (sources.size() < total);

    my::Scheduler<node_t, partition_t, depth_t> &&scheduler_c = my::CommonScheduler<node_t, partition_t, depth_t>(partition);
    my::Scheduler<node_t, partition_t, depth_t> &&scheduler_p = my::PriorityScheduler<node_t, partition_t, depth_t>(parq);
    my::BFS<node_t, partition_t, depth_t> bfs_c{csr, partition, scheduler_c};
    my::BFS<node_t, partition_t, depth_t> bfs_p{csr, partition, scheduler_p};

    for (int i = 0; i < total; ++i) {
        int source = sources[i];
        // int source = i;
        scheduler_c.add_active_v(source, 0);
        scheduler_p.add_active_v(source, 0);
        bfs_c.run_repeated();
        bfs_p.run_repeated();
        std::cout << source << " " << bfs_c.get_switching_count() << " "
                << bfs_p.get_switching_count() << std::endl;
        avg_switches += bfs_c.get_switching_count();
        long long scc = bfs_c.get_switching_count();
        long long scp = bfs_p.get_switching_count();
        if (scc > scp) {
            positive++;
            avg_gain += scc - scp;
            if (scc == scp) { equal++; }
        }
        bfs_c.reset();
        bfs_p.reset();
    }
    avg_gain = avg_gain / positive;

    std::cout << "Positive/Equal/Negative " << positive << "/" << equal << "/" << total - positive - equal << std::endl;
    std::cout << "Average Gain/Switch: " << avg_gain << "/" << avg_switches/total << std::endl;
    return 0;
}
