#pragma once

#include "Scheduler.h"
#include "../ParQ.h"

namespace my {
    template<typename _Node, typename _PIdx, typename _Prop>
    class PriorityScheduler : public my::Scheduler<_Node, _PIdx, _Prop> {
    private:
        my::ParQ<_Node, _PIdx> &parq;
    public:
        PriorityScheduler(my::ParQ<_Node, _PIdx> &_parq);
        ~PriorityScheduler();

        _PIdx next_partition();
        void reset() {
            parq.reset();
            Scheduler<_Node, _PIdx, _Prop>::reset();
        }
        void add_active_v(_Node n, _Prop p);
        std::vector<std::pair<_Node, _Prop>> get_active_set(_PIdx pid);
    };
}

template<typename _Node, typename _PIdx, typename _Prop>
my::PriorityScheduler<_Node, _PIdx, _Prop>::PriorityScheduler(my::ParQ<_Node, _PIdx> &_parq)
    : my::Scheduler<_Node, _PIdx, _Prop>{_parq.get_partition()}, parq{_parq} {
}

template<typename _Node, typename _PIdx, typename _Prop>
my::PriorityScheduler<_Node, _PIdx, _Prop>::~PriorityScheduler() {}

template<typename _Node, typename _PIdx, typename _Prop>
_PIdx my::PriorityScheduler<_Node, _PIdx, _Prop>::next_partition() {
    std::vector<double> tmp_scores(this->partition.get_partition_number(), 0.0);
    _PIdx partition_number = this->partition.get_partition_number();
    #pragma omp parallel for
    for (_PIdx i = 0; i < partition_number; ++i) {
        double s = 0.0;
        std::unordered_map<_Node, _Prop> &active_set = this->active_sets[i];
        int valid{0};
        for (auto &[n,d] : active_set) {
            if (parq[n] >= 0.0) {
                s += parq[n];
                valid++;
            }
        }
        // tmp_scores[i] = s / active_set.size(); // TMP / |H0|
        if (valid) {
            tmp_scores[i] = s / valid; // TMP / |H0|
        } else {
            tmp_scores[i] = 0.0;
        }
    }
    _PIdx idx{-1};
    double max_score{0.0};
    for (_PIdx i = 0; i < partition_number; ++i) {
        if (tmp_scores[i] > max_score) {
            max_score = tmp_scores[i];
            idx = i;
        }
    }
    // fallback: using active vertex number as priority
    if (max_score - 0.0 < 1e-5) {
        size_t max_size = 0;
        for (_PIdx i = 0; i < partition_number; ++i) {
            if (this->active_sets[i].size() >= max_size) {
                idx = i;
                max_size = this->active_sets[i].size();
            }
        }
        if (max_size == 0) {
            return -1; // algorithm terminated
        }
    }
    assert(idx >= 0);
    
    return idx;
}

template<typename _Node, typename _PIdx, typename _Prop>
void my::PriorityScheduler<_Node, _PIdx, _Prop>::add_active_v(_Node n, _Prop p) {
    std::unordered_map<_Node, _Prop> &tmp= this->active_sets[this->partition[n]];
    if (tmp.count(n) == 0 || p < tmp[n]) {
        tmp[n] = p;
    }
}

template<typename _Node, typename _PIdx, typename _Prop>
std::vector<std::pair<_Node, _Prop>> my::PriorityScheduler<_Node, _PIdx, _Prop>::get_active_set(_PIdx pid) {
    std::vector<std::pair<_Node, _Prop>> res;
    for (auto &[n,d] : this->active_sets[pid]) {
        res.emplace_back(n, d);
        parq.set_negative(n);
    }
    std::sort(res.begin(), res.end(), 
            [](const std::pair<_Node, _Prop> &lhs, const std::pair<_Node, _Prop> &rhs) 
                { return lhs.second < rhs.second; });
    this->active_sets[pid].clear();
    return res;
}