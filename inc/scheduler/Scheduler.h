#pragma once

#include "../Partition.h"
#include <vector>
#include <unordered_map>

namespace my {
    template<typename _Node, typename _PIdx, typename _Prop>
    class Scheduler {
    protected:
        my::Partition<_Node, _PIdx> &partition;
        std::unordered_map<_Node, _Prop> *active_sets;
    public:
        Scheduler(my::Partition<_Node, _PIdx> &p);
        virtual ~Scheduler();

        virtual _PIdx next_partition()=0;
        virtual void reset();
        virtual void add_active_v(_Node n, _Prop p);
        virtual std::vector<std::pair<_Node, _Prop>> get_active_set(_PIdx pid);
    };
}

template<typename _Node, typename _PIdx, typename _Prop>
my::Scheduler<_Node, _PIdx, _Prop>::Scheduler(my::Partition<_Node, _PIdx> &p) : partition{p} {
    active_sets = new std::unordered_map<_Node, _Prop>[p.get_partition_number()];
}

template<typename _Node, typename _PIdx, typename _Prop>
my::Scheduler<_Node, _PIdx, _Prop>::~Scheduler() {
    if (active_sets) delete[] active_sets;
}

template<typename _Node, typename _PIdx, typename _Prop>
void my::Scheduler<_Node, _PIdx, _Prop>::reset() {
    for (_PIdx i = 0; i < partition.get_partition_number(); ++i) {
        active_sets[i].clear();
    }
}

template<typename _Node, typename _PIdx, typename _Prop>
void my::Scheduler<_Node, _PIdx, _Prop>::add_active_v(_Node n, _Prop p) {
    std::unordered_map<_Node, _Prop> &tmp= active_sets[partition[n]];
    tmp[n] = p;
}

template<typename _Node, typename _PIdx, typename _Prop>
std::vector<std::pair<_Node, _Prop>> my::Scheduler<_Node, _PIdx, _Prop>::get_active_set(_PIdx pid) {
    std::unordered_map<_Node, _Prop> &tmp= active_sets[pid];
    std::vector<std::pair<_Node, _Prop>> res;
    for (auto &[n,p] : tmp) {
        res.emplace_back(n, p);
    }
}