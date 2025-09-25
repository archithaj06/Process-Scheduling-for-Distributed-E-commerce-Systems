// slo_scheduler.cpp
// Single-file C++17 program: Multithreaded-style scheduler simulation with EDF + Aging and SLO metrics.
// Build: g++ -std=gnu++17 -O2 -pthread slo_scheduler.cpp -o slo_scheduler
// Run examples:
//   ./slo_scheduler --n 120 --burst
//   ./slo_scheduler --n 80 --seed 42

#include <bits/stdc++.h>
using namespace std;

// ====================== Data Structures ======================
using Clock = std::chrono::steady_clock;

enum RejectReason { NONE, INSUFFICIENT_RESOURCES, FORCED_TO_WAIT, DEADLINE_EXCEEDED };

struct SLODefaults {
    // Default relative deadlines per transaction type (in ms)
    int payment_ms  = 300;
    int checkout_ms = 500;
    int default_ms  = 2000;
};

struct ThreadUnit {
    string id;        // e.g., "S1-T0"
    int resources;    // capacity units
    int priority;     // higher value = preferred when multiple threads are free
    bool isBusy = false;
    int completionTime = 0; // simulated absolute ms timestamp when it frees
};

struct Service {
    string name;
    vector<ThreadUnit> workerThreads;
};

struct Request {
    string transactionType; // e.g., "payment", "browse", "checkout"
    int requiredResources;
    int arrivalTime;        // ms
    int startTime = 0;      // ms
    int waitingTime = 0;    // ms
    int turnaroundTime = 0; // ms
    bool isProcessed = false;
    RejectReason rejectReason = NONE;
    string assignedThreadId;
    bool countedForcedWait = false;
    bool countedBlocked = false;
    // SLO: relative deadline (ms). If 0, inferred from defaults
    int relDeadlineMs = 0;

    Request() = default;
    Request(string tt, int rr, int at)
        : transactionType(std::move(tt)), requiredResources(rr), arrivalTime(at) {}
};

// ====================== Globals & Helpers ======================
static queue<Request*> waitingQueue;      // waiting requests (pointers into vector<Request>)
static const int PROC_TIME_MS = 3;        // fixed processing time per job in this simulation
static const double AGING_BONUS_PER_MS = 0.0005; // higher => more fairness (lowers urgency score)

int inferDefaultDeadlineMs(const string& tt, const SLODefaults& slo) {
    if (tt == "payment")  return slo.payment_ms;
    if (tt == "checkout") return slo.checkout_ms;
    return slo.default_ms;
}

// Urgency: smaller is better. Combines (1/slack) with an aging bonus to prevent starvation.
double urgencyScore(const Request* req, int now) {
    int waited = max(0, now - req->arrivalTime);
    int slack  = max(1, req->relDeadlineMs - waited);
    double base = 1.0 / (double)slack;             // earlier deadlines => larger base
    double aged = max(0.0, base - AGING_BONUS_PER_MS * waited);
    return aged;
}

// ====================== Scheduling Core ======================
void processWaitingQueue(vector<Service>& services,
                         map<string,int>& transactionToService,
                         int currentTime, int &blockedCount,
                         const SLODefaults& slo) {
    queue<Request*> rest;

    // Drain the waiting queue into a temporary vector for selection
    vector<Request*> pending;
    while (!waitingQueue.empty()) { pending.push_back(waitingQueue.front()); waitingQueue.pop(); }

    // Filter out immediate deadline-missed requests; ensure deadlines assigned
    vector<Request*> stillPending;
    stillPending.reserve(pending.size());
    for (auto* req : pending) {
        if (!req->relDeadlineMs) req->relDeadlineMs = inferDefaultDeadlineMs(req->transactionType, slo);
        int waited = max(0, currentTime - req->arrivalTime);
        if (waited >= req->relDeadlineMs) {
            if (!req->isProcessed) {
                req->isProcessed = false;
                req->rejectReason = DEADLINE_EXCEEDED;
            }
            continue; // drop
        }
        stillPending.push_back(req);
    }

    // Greedy pass: try to schedule as many as possible now.
    // For each service, we repeatedly pick the most urgent request that fits a free thread.
    bool anyScheduled = true;
    while (anyScheduled) {
        anyScheduled = false;

        // Group requests by service index to pick locally-best request
        unordered_map<int, vector<Request*>> byService;
        for (auto* req : stillPending) {
            auto it = transactionToService.find(req->transactionType);
            if (it == transactionToService.end()) continue;
            byService[it->second].push_back(req);
        }

        // For each service, attempt to place at least one request
        vector<Request*> newlyPlaced;
        for (auto& [svcIdx, reqs] : byService) {
            Service& service = services[svcIdx];

            // Find a free thread (highest priority) and the most urgent request that can fit it
            int threadIdx = -1, bestPrio = -1;
            for (int i = 0; i < (int)service.workerThreads.size(); ++i) {
                auto& th = service.workerThreads[i];
                if (!th.isBusy) {
                    if (th.priority > bestPrio) { bestPrio = th.priority; threadIdx = i; }
                }
            }
            if (threadIdx == -1) continue; // no free thread in this service

            // Among reqs that fit this service capacity, pick min urgency score
            Request* bestReq = nullptr;
            double bestScore = numeric_limits<double>::infinity();
            for (auto* r : reqs) {
                // check capacity on the chosen thread
                if (service.workerThreads[threadIdx].resources >= r->requiredResources) {
                    double sc = urgencyScore(r, currentTime);
                    if (sc < bestScore) {
                        bestScore = sc;
                        bestReq = r;
                    }
                }
            }
            if (!bestReq) continue;

            // Schedule bestReq on threadIdx
            auto& thread = service.workerThreads[threadIdx];
            thread.isBusy = true;
            thread.completionTime = currentTime + PROC_TIME_MS;

            bestReq->startTime = max(currentTime, bestReq->arrivalTime);
            bestReq->waitingTime = max(0, bestReq->startTime - bestReq->arrivalTime);
            bestReq->turnaroundTime = bestReq->waitingTime + PROC_TIME_MS;
            bestReq->isProcessed = true;
            bestReq->assignedThreadId = thread.id;

            newlyPlaced.push_back(bestReq);
            anyScheduled = true;
        }

        // Remove newly placed from stillPending
        if (!newlyPlaced.empty()) {
            vector<Request*> remain;
            remain.reserve(stillPending.size());
            unordered_set<Request*> placed(newlyPlaced.begin(), newlyPlaced.end());
            for (auto* r : stillPending) if (!placed.count(r)) remain.push_back(r);
            stillPending.swap(remain);
        }
    }

    // Anything left goes back to waiting (blocked this tick)
    for (auto* req : stillPending) {
        if (!req->countedBlocked) { blockedCount++; req->countedBlocked = true; }
        rest.push(req);
    }
    waitingQueue = std::move(rest);
}

void processRequests(vector<Service>& services,
                     vector<Request>& requests,
                     map<string,int>& transactionToService,
                     const SLODefaults& slo) {
    // Determine simulation horizon
    int maxArrival = 0;
    for (auto& r : requests) maxArrival = max(maxArrival, r.arrivalTime);
    int currentTime = 0;
    int blockedCount = 0;

    // Main time-stepped simulation
    size_t nextReqIdx = 0;
    const int END_TIME = maxArrival + 5000; // tail room

    while (currentTime <= END_TIME) {
        // 1) Free any completed threads
        for (auto& svc : services) {
            for (auto& th : svc.workerThreads) {
                if (th.isBusy && th.completionTime <= currentTime) {
                    th.isBusy = false;
                }
            }
        }

        // 2) Admit arrivals at currentTime
        while (nextReqIdx < requests.size() && requests[nextReqIdx].arrivalTime <= currentTime) {
            Request& req = requests[nextReqIdx];
            if (!req.relDeadlineMs) req.relDeadlineMs = inferDefaultDeadlineMs(req.transactionType, slo);

            // Can we place it immediately?
            bool placed = false;
            auto it = transactionToService.find(req.transactionType);
            if (it != transactionToService.end()) {
                Service& svc = services[it->second];
                int bestIdx = -1, bestPrio = -1;
                for (int i = 0; i < (int)svc.workerThreads.size(); ++i) {
                    auto& th = svc.workerThreads[i];
                    if (!th.isBusy && th.resources >= req.requiredResources) {
                        if (th.priority > bestPrio) { bestPrio = th.priority; bestIdx = i; }
                    }
                }
                if (bestIdx != -1) {
                    auto& th = svc.workerThreads[bestIdx];
                    th.isBusy = true;
                    th.completionTime = currentTime + PROC_TIME_MS;
                    req.startTime = currentTime;
                    req.waitingTime = currentTime - req.arrivalTime;
                    req.turnaroundTime = req.waitingTime + PROC_TIME_MS;
                    req.isProcessed = true;
                    req.assignedThreadId = th.id;
                    placed = true;
                }
            } else {
                req.rejectReason = INSUFFICIENT_RESOURCES;
            }

            if (!placed && req.rejectReason != INSUFFICIENT_RESOURCES) {
                // If not placed now, check immediate deadline miss
                int waitedNow = max(0, currentTime - req.arrivalTime);
                if (!req.relDeadlineMs) req.relDeadlineMs = inferDefaultDeadlineMs(req.transactionType, slo);
                if (waitedNow >= req.relDeadlineMs) {
                    req.isProcessed = false;
                    req.rejectReason = DEADLINE_EXCEEDED;
                } else {
                    waitingQueue.push(&req);
                    req.rejectReason = FORCED_TO_WAIT;
                    req.countedForcedWait = true;
                }
            }
            ++nextReqIdx;
        }

        // 3) Try to schedule waiting jobs (EDF + Aging per service)
        processWaitingQueue(services, transactionToService, currentTime, blockedCount, slo);

        // 4) Check termination: no pending arrivals, no waiting, all threads idle
        bool anyBusy = false;
        for (auto& s : services) for (auto& t : s.workerThreads) anyBusy |= t.isBusy;
        if (nextReqIdx >= requests.size() && waitingQueue.empty() && !anyBusy) break;

        currentTime += 1; // advance simulation clock by 1ms
    }
}

// ====================== Metrics & Output ======================
void calculateMetrics(const vector<Request>& requests) {
    long long total_wait = 0, total_turn = 0;
    int processed = 0;
    int resRejects = 0, forcedWait = 0, blocked = 0, deadlineMiss = 0;

    cout << "Processing order / outcomes:\n";
    for (auto& req : requests) {
        if (req.isProcessed) {
            cout << "[" << req.transactionType << " (Needs: " << req.requiredResources
                 << ", Deadline: " << req.relDeadlineMs << "ms)] - Processed by "
                 << req.assignedThreadId << " (Arr: " << req.arrivalTime
                 << "ms, Start: " << req.startTime
                 << "ms, Wait: " << req.waitingTime << "ms)\n";
            total_wait += req.waitingTime;
            total_turn += req.turnaroundTime;
            processed++;
        } else {
            string reason = "Blocked";
            if (req.rejectReason == INSUFFICIENT_RESOURCES) reason = "Insufficient Resources";
            else if (req.rejectReason == DEADLINE_EXCEEDED) reason = "Deadline Missed";
            else if (req.rejectReason == FORCED_TO_WAIT) reason = "Forced To Wait";
            cout << "[" << req.transactionType << " (Needs: " << req.requiredResources
                 << ", Deadline: " << req.relDeadlineMs << "ms)] - Rejected (" << reason << ")\n";
            if (req.rejectReason == INSUFFICIENT_RESOURCES) resRejects++;
            else if (req.rejectReason == DEADLINE_EXCEEDED) deadlineMiss++;
            else if (req.rejectReason == FORCED_TO_WAIT) blocked++; // never scheduled in time
            else blocked++;
        }
        if (req.countedForcedWait) forcedWait++;
    }

    cout << "\n=== System Metrics ===\n";
    cout << "Total processed: " << processed << "/" << requests.size() << "\n";
    cout << "Average waiting time: " << (processed ? (double)total_wait/processed : 0.0) << " ms\n";
    cout << "Average turnaround time: " << (processed ? (double)total_turn/processed : 0.0) << " ms\n";
    cout << "Rejected (insufficient resources): " << resRejects << "\n";
    cout << "Rejected (deadline missed): " << deadlineMiss << "\n";
    cout << "Forced to wait at least once: " << forcedWait << "\n";
    cout << "Blocked (other): " << blocked << "\n";
}

// ====================== Workload Generation ======================
struct WorkloadCfg {
    int N = 100;          // number of requests
    bool burst = false;   // add periodic bursts
    int seed = 12345;
};

vector<Request> generateWorkload(const WorkloadCfg& cfg) {
    mt19937 rng(cfg.seed);
    vector<Request> out; out.reserve(cfg.N);

    // Base rates for transaction types
    vector<string> types = {"payment","checkout","browse"};
    vector<double> probs = {0.2, 0.3, 0.5}; // sum to 1

    // Resource requirements (units)
    uniform_int_distribution<int> resPay(2,3), resCheck(2,4), resBrowse(1,2);

    // Arrival pattern
    int t = 0;
    for (int i = 0; i < cfg.N; ++i) {
        // Pick type
        double u = std::generate_canonical<double, 10>(rng);
        string tt;
        if (u < probs[0]) tt = "payment";
        else if (u < probs[0]+probs[1]) tt = "checkout";
        else tt = "browse";

        // Resources
        int reqRes = 1;
        if (tt == "payment") reqRes = resPay(rng);
        else if (tt == "checkout") reqRes = resCheck(rng);
        else reqRes = resBrowse(rng);

        // Arrival increment: steady 1–6 ms, with optional bursts every 200ms for 40ms
        int dt = 1 + (rng() % 6);
        if (cfg.burst) {
            if ((t % 200) < 40) {
                dt = 0; // many arrive at same time in burst window
            }
        }
        t += dt;

        out.emplace_back(tt, reqRes, t);
    }
    return out;
}

// ====================== Setup Example Services ======================
vector<Service> makeExampleServices() {
    vector<Service> svcs;
    // Service 0: Payment
    {
        Service s; s.name = "Payment";
        s.workerThreads.push_back({"S0-T0", 3, 3});
        s.workerThreads.push_back({"S0-T1", 3, 2});
        svcs.push_back(s);
    }
    // Service 1: Checkout
    {
        Service s; s.name = "Checkout";
        s.workerThreads.push_back({"S1-T0", 4, 2});
        s.workerThreads.push_back({"S1-T1", 3, 1});
        svcs.push_back(s);
    }
    // Service 2: Browse / Catalog
    {
        Service s; s.name = "Browse";
        s.workerThreads.push_back({"S2-T0", 2, 2});
        s.workerThreads.push_back({"S2-T1", 2, 1});
        svcs.push_back(s);
    }
    return svcs;
}

map<string,int> makeTxnToServiceMap() {
    // Map transaction type to service index
    return {
        {"payment", 0},
        {"checkout", 1},
        {"browse", 2},
    };
}

// ====================== CLI Parsing ======================
WorkloadCfg parseCLI(int argc, char** argv) {
    WorkloadCfg cfg;
    for (int i = 1; i < argc; ++i) {
        string a = argv[i];
        if (a == "--n" && i+1 < argc) { cfg.N = stoi(argv[++i]); }
        else if (a == "--burst") { cfg.burst = true; }
        else if (a == "--seed" && i+1 < argc) { cfg.seed = stoi(argv[++i]); }
        else if (a == "--help") {
            cerr << "Usage: ./slo_scheduler [--n N] [--burst] [--seed S]\n";
            exit(0);
        }
    }
    return cfg;
}

// ====================== Main ======================
int main(int argc, char** argv) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    // Parse CLI
    WorkloadCfg cfg = parseCLI(argc, argv);

    // Build services and mapping
    vector<Service> services = makeExampleServices();
    map<string,int> txnMap = makeTxnToServiceMap();

    // Generate workload
    vector<Request> requests = generateWorkload(cfg);

    // SLO defaults
    SLODefaults slo; // use defaults; edit if needed

    // Run simulation
    processRequests(services, requests, txnMap, slo);

    // Report
    calculateMetrics(requests);

    return 0;
}
