#pragma once

#include "CSR.h"
#include "Partition.h"
#include "scheduler/Scheduler.h"
#include <queue>
#include <iterator>

namespace my {
    template<typename _Node, typename _PIdx, typename _Dep>
    class BFS {
    private:
        my::Partition<_Node, _PIdx> &partition;
        my::CSR<_Node> &csr;
        my::Scheduler<_Node, _PIdx, _Dep> &scheduler;
        _Node vertex_number;
        _Dep *depth;

        long long switching_count;
    public:
        BFS(my::CSR<_Node> &c,
            my::Partition<_Node, _PIdx> &p,
            my::Scheduler<_Node, _PIdx, _Dep> &s);
        ~BFS();

        long long get_switching_count() const { return switching_count; }

        void run_sync();
        void run_repeated();

        void reset() {
            std::fill(depth, depth+vertex_number, -1);
            switching_count = 0;
            scheduler.reset();
        }
    };
}

template<typename _Node, typename _PIdx, typename _Dep>
my::BFS<_Node, _PIdx, _Dep>::BFS(CSR<_Node> &c,
        Partition<_Node, _PIdx> &p,
        Scheduler<_Node, _PIdx, _Dep> &s)
    : csr{c}, partition{p}, scheduler{s}, switching_count{0}, vertex_number{c.get_vertex_number()} {
    depth = new _Dep[csr.get_vertex_number()];
    std::fill(depth, depth+vertex_number, -1);
}

template<typename _Node, typename _PIdx, typename _Dep>
my::BFS<_Node, _PIdx, _Dep>::~BFS() {
    if (depth) delete[] depth;
}

template<typename _Node, typename _PIdx, typename _Dep>
void my::BFS<_Node, _PIdx, _Dep>::run_repeated() {
    using _Task = std::pair<_Node, _Dep>;
    using Iter = typename my::CSR<_Node>::edge_iter;

    _PIdx cur_p = scheduler.next_partition();
    while (cur_p != -1) {
        std::vector<std::pair<_Node, _Dep>> frontier = scheduler.get_active_set(cur_p);
        std::queue<_Task> q;
        for (auto &[n,d] : frontier) {
            q.emplace(n,d);
        }
        while (!q.empty()) {
            auto &[n,d] = q.front();
            if (depth[n] == -1 || d < depth[n]) {
                depth[n] = d;
                // scatter
                for (Iter it = csr.begin(n); it < csr.end(n); ++it) {
                    if (partition[*it] != cur_p) {
                        if (depth[*it]==-1 || depth[*it]>d+1) {
                            scheduler.add_active_v(*it, d+1);
                        }
                    } else {
                        q.emplace(*it, d+1);
                    }
                }
            }
            q.pop();
        }
        int prev_p = cur_p;
        cur_p = scheduler.next_partition();
        if (prev_p != cur_p)
            switching_count++;
    }
}


template<typename _Node, typename _PIdx, typename _Dep>
void my::BFS<_Node, _PIdx, _Dep>::run_sync() {
    using _Task = std::pair<_Node, _Dep>;
    using Iter = typename my::CSR<_Node>::edge_iter;

    _PIdx cur_p = scheduler.next_partition();
    while (cur_p != -1) {
        std::vector<std::pair<_Node, _Dep>> frontier = scheduler.get_active_set(cur_p);
        for (auto &[n,d] : frontier) {
            if (depth[n] == -1 || depth[n] > d) {
                depth[n] = d;
                for (Iter it = csr.begin(n); it < csr.end(n); ++it) {
                    scheduler.add_active_v(*it, d+1);
                }
            }
        }
        int prev_p = cur_p;
        cur_p = scheduler.next_partition();
        if (prev_p != cur_p)
            switching_count++;
    }
}