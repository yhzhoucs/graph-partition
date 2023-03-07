#pragma once

#include "Scheduler.h"

namespace my {
    template<typename _Node, typename _PIdx, typename _Prop>
    class CommonScheduler : public Scheduler<_Node, _PIdx, _Prop> {
    public:
        CommonScheduler(my::Partition<_Node, _PIdx> &p);
        ~CommonScheduler();

        _PIdx next_partition();
        void add_active_v(_Node n, _Prop p);
        std::vector<std::pair<_Node, _Prop>> get_active_set(_PIdx pid);
    };
}    

template<typename _Node, typename _PIdx, typename _Prop>
my::CommonScheduler<_Node, _PIdx, _Prop>::CommonScheduler(my::Partition<_Node, _PIdx> &p)
    : Scheduler<_Node, _PIdx, _Prop>{p} {}

template<typename _Node, typename _PIdx, typename _Prop>
my::CommonScheduler<_Node, _PIdx, _Prop>::~CommonScheduler() {}

template<typename _Node, typename _PIdx, typename _Prop>
_PIdx my::CommonScheduler<_Node, _PIdx, _Prop>::next_partition() {
    _PIdx idx{-1};
    size_t max_size = 0;
    for (_PIdx i = 0; i < this->partition.get_partition_number(); ++i) {
        if (this->active_sets[i].size() >= max_size) {
            idx = i;
            max_size = this->active_sets[i].size();
        }
    }
    if (max_size == 0) {
        return -1; // algorithm terminated
    }
    assert(idx >= 0);
    return idx;
}

template<typename _Node, typename _PIdx, typename _Prop>
void my::CommonScheduler<_Node, _PIdx, _Prop>::add_active_v(_Node n, _Prop p) {
    std::unordered_map<_Node, _Prop> &tmp= this->active_sets[this->partition[n]];
    if (tmp.count(n) == 0 || p < tmp[n]) {
        tmp[n] = p;
    }
}

template<typename _Node, typename _PIdx, typename _Prop>
std::vector<std::pair<_Node, _Prop>> my::CommonScheduler<_Node, _PIdx, _Prop>::get_active_set(_PIdx pid) {
    std::vector<std::pair<_Node, _Prop>> res;
    for (auto &[n,d] : this->active_sets[pid]) {
        res.emplace_back(n, d);
    }
    std::sort(res.begin(), res.end(), 
            [](const std::pair<_Node, _Prop> &lhs, const std::pair<_Node, _Prop> &rhs) 
                { return lhs.second < rhs.second; });
    this->active_sets[pid].clear();
    return res;
}