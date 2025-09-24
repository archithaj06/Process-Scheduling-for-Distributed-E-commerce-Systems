#include <iostream>
#include <queue>
#include <vector>
#include <string>
#include <map>
#include <algorithm>

using namespace std;

enum RejectReason { NONE, INSUFFICIENT_RESOURCES, FORCED_TO_WAIT };

class WorkerThread {
public:
    string id;
    int priority;
    int resources;
    bool isBusy;
    int completionTime;

    WorkerThread(string i, int p, int r)
        : id(i), priority(p), resources(r), isBusy(false), completionTime(0) {}
};

class Request {
public:
    string transactionType;
    int requiredResources;
    int arrivalTime;
    int startTime;
    int waitingTime;
    int turnaroundTime;
    bool isProcessed;
    RejectReason rejectReason;
    string assignedThreadId;
    bool countedForcedWait = false;
    bool countedBlocked = false;

    Request(string tt, int rr, int at)
        : transactionType(tt), requiredResources(rr), arrivalTime(at),
          startTime(0), waitingTime(0), turnaroundTime(0),
          isProcessed(false), rejectReason(NONE), assignedThreadId("") {}
};

class Service {
public:
    vector<WorkerThread> workerThreads;
};

queue<Request*> waitingQueue;

void processWaitingQueue(vector<Service>& services, map<string, int>& transactionToService, int currentTime, int &blockedCount) {
    queue<Request*> tempQueue;

    while (!waitingQueue.empty()) {
        Request* req = waitingQueue.front();
        waitingQueue.pop();

        // Validate transaction type exists
        if (transactionToService.find(req->transactionType) == transactionToService.end()) {
            req->rejectReason = INSUFFICIENT_RESOURCES;
            if (!req->countedBlocked) {
                blockedCount++;
                req->countedBlocked = true;
            }
            continue;
        }

        Service& service = services[transactionToService[req->transactionType]];
        priority_queue<pair<int, int>> pq;

        // Collect available threads
        for (int i = 0; i < service.workerThreads.size(); ++i) {
            auto& thread = service.workerThreads[i];
            if (!thread.isBusy && thread.resources >= req->requiredResources) {
                pq.push({thread.priority, i});
            }
        }

        if (!pq.empty()) {
            int threadIdx = pq.top().second;
            auto& thread = service.workerThreads[threadIdx];
            thread.isBusy = true;
            thread.completionTime = currentTime + 3;

            req->startTime = max(currentTime, req->arrivalTime); // Ensure valid start time
            req->waitingTime = max(0, req->startTime - req->arrivalTime); // Prevent negative waiting time
            req->turnaroundTime = req->waitingTime + 3; // Assume fixed processing time
            req->isProcessed = true;
            req->assignedThreadId = thread.id;
        } else {
            tempQueue.push(req);
            if (!req->countedBlocked) {
                blockedCount++;
                req->countedBlocked = true;
            }
        }
    }
    waitingQueue = tempQueue; // Update waiting queue
}

void processRequests(vector<Service>& services, vector<Request>& requests, map<string, int>& transactionToService) {
    int currentTime = 0;
    int blockedCount = 0;

    for (auto& req : requests) {
        // Update thread availability based on completion times
        for (auto& service : services) {
            for (auto& thread : service.workerThreads) {
                if (thread.isBusy && currentTime >= thread.completionTime) {
                    thread.isBusy = false;
                }
            }
        }

        // Handle unknown transaction types
        if (!transactionToService.count(req.transactionType)) {
            req.isProcessed = false;
            req.rejectReason = INSUFFICIENT_RESOURCES;
            continue;
        }

        Service& service = services[transactionToService[req.transactionType]];
        bool hasResource = false, hasAvailable = false;

        // Check resource availability
        for (auto& thread : service.workerThreads) {
            if (thread.resources >= req.requiredResources) {
                hasResource = true;
                if (!thread.isBusy) hasAvailable = true;
            }
        }

        if (!hasResource) {
            req.isProcessed = false;
            req.rejectReason = INSUFFICIENT_RESOURCES;
        } else if (!hasAvailable) {
            waitingQueue.push(&req);
            if (!req.countedForcedWait) {
                req.rejectReason = FORCED_TO_WAIT;
                req.countedForcedWait = true;
            }
        } else {
            priority_queue<pair<int, int>> pq;

            for (int i = 0; i < service.workerThreads.size(); ++i) {
                auto& thread = service.workerThreads[i];
                if (!thread.isBusy && thread.resources >= req.requiredResources) {
                    pq.push({thread.priority, i});
                }
            }

            if (!pq.empty()) {
                int idx = pq.top().second;
                auto& thread = service.workerThreads[idx];
                thread.isBusy = true;
                thread.completionTime = currentTime + 3;

                req.startTime = max(currentTime, req.arrivalTime);
                req.waitingTime = max(0, req.startTime - req.arrivalTime);
                req.turnaroundTime = req.waitingTime + 3; // Assume fixed process duration
                req.isProcessed = true;
                req.assignedThreadId = thread.id;
            }
        }

        processWaitingQueue(services, transactionToService, currentTime, blockedCount);
        currentTime++; // Increment time AFTER processing
    }

    // Final cleanup for remaining requests
    for (int extra = 0; extra < 100; ++extra) {
        bool allDone = true;

        for (auto& service : services) {
            for (auto& thread : service.workerThreads) {
                if (thread.isBusy && currentTime >= thread.completionTime) {
                    thread.isBusy = false;
                }
            }
            
            processWaitingQueue(services, transactionToService, currentTime, blockedCount);
            
            if (!waitingQueue.empty()) allDone = false; 
        }

        currentTime++;
        
        if (allDone) break; 
    }
}

void calculateMetrics(const vector<Request>& requests) {
    int total_wait = 0, total_turn = 0, processed = 0, resRejects = 0, forcedWait = 0, blocked = 0;

    cout << "\nProcessing order:\n";
    for (auto& req : requests) {
        if (req.isProcessed) {
            cout << "[" << req.transactionType << " (Needs: " << req.requiredResources << ")] - Processed by " 
                 << req.assignedThreadId << " (Waited: " << req.waitingTime << "ms)\n";
            total_wait += req.waitingTime;
            total_turn += req.turnaroundTime;
            processed++;
        } else {
            string reason = req.rejectReason == INSUFFICIENT_RESOURCES ? 
                          "Insufficient Resources" : "Blocked";
            cout << "[" << req.transactionType << " (Needs: " << req.requiredResources << ")] - Rejected (" 
                 << reason << ")\n";
            if (req.rejectReason == INSUFFICIENT_RESOURCES) resRejects++;
            else blocked++;
        }
        if (req.countedForcedWait) forcedWait++;
    }

    cout << "\n=== System Metrics ==="
         << "\nAverage waiting time: " << (processed ? (double)total_wait/processed : 0)
         << "\nAverage turnaround time: " << (processed ? (double)total_turn/processed : 0)
         << "\nRejected requests (resources): " << resRejects
         << "\nForced to wait: " << forcedWait
         << "\nBlocked requests: " << blocked
         << "\nTotal processed: " << processed << "/" << requests.size() << endl;
}

int main() {
    vector<Service> services;
    int n, m, p, r, time = 0;
    string serviceType;

    cout << "Enter number of services: ";
    cin >> n;

    map<string, int> transactionToService;

    for (int i = 0; i < n; ++i) {
        Service s;
        cout << "Enter type of transaction for Service " << i+1 << ": ";
        cin >> serviceType;
        transactionToService[serviceType] = i;

        cout << "Enter number of worker threads for Service " << i+1 << ": ";
        cin >> m;

        for (int j = 0; j < m; ++j) {
            cout << "Priority and resources for Thread " << j+1 << ": ";
            cin >> p >> r;
            s.workerThreads.emplace_back("S" + to_string(i+1) + "T" + to_string(j+1), p, r);
        }
        services.push_back(s);
    }

    vector<Request> requests;
    string tt;
    int res;

    cout << "\nEnter requests (transaction_type resources). Type 'exit' to finish:\n";
    while (cin >> tt && tt != "exit") {
        cin >> res;
        requests.emplace_back(tt, res, time++);
    }

    processRequests(services, requests, transactionToService);
    calculateMetrics(requests);

    return 0;
}

