#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <chrono>
#include <iostream>

using namespace std;

constexpr int NUM_PRODUCERS = 6;
constexpr int NUM_ITEMS_PER_PRODUCER = 100;

std::atomic<bool> producersDone{false};

struct Node;

struct TaggedPtr {
    Node* ptr;
    uint64_t version;

    bool operator==(const TaggedPtr& other) const {
        return ptr == other.ptr && version == other.version;
    }

    bool operator!=(const TaggedPtr& other) const {
        return !(*this == other);
    }
};

struct Node {
    string values;
    atomic<TaggedPtr> next;

    Node(string val) {
        values = val;
        next.store({nullptr, 0});
    }
};

struct Queue {
    alignas(64) atomic<TaggedPtr> head;
    alignas(64) atomic<TaggedPtr> tail;
} Q;

// Wrapper around atomic CAS
inline bool CAS(atomic<TaggedPtr>* addr,
                TaggedPtr& expected,
                TaggedPtr desired)
{
    return addr->compare_exchange_strong(expected, desired);
}

bool init() {
    Node* dummy = new Node("0");

    TaggedPtr taggedDummy = {dummy, 0};

    Q.head.store(taggedDummy);
    Q.tail.store(taggedDummy);

    return true;
}

bool enqueue(string str) {
    // E1: Create a new node
    Node* newNode = new Node(str);

    TaggedPtr localTail;
    TaggedPtr localNext;

    // E2: Loop until we are not able to do a successful enqueue
    while (true) {

        // E3: Fetch the tail
        localTail = Q.tail.load();

        // E4: Fetch the next pointer
        localNext = localTail.ptr->next.load();

        // E5: Tail moved forward just skip
        if (localTail == Q.tail.load()) {

            // E6: Check if next is still null
            // -> which means no other faster thread enqueued before us
            if (localNext.ptr == nullptr) {

                TaggedPtr desiredNext;
                desiredNext.ptr = newNode;
                desiredNext.version = localNext.version + 1;

                // E7: CAS and come out,
                // but notice tail is still at the same place
                if (CAS(&localTail.ptr->next,
                        localNext,
                        desiredNext)) {
                    break;
                }

            } else {
                // Some other faster thread added a new node before us

                // E8: Just move the tail for now
                // and then will come back
                TaggedPtr desiredTail;
                desiredTail.ptr = localNext.ptr;
                desiredTail.version = localTail.version + 1;

                CAS(&Q.tail, localTail, desiredTail);
            }
        }
    }

    // E9: Best effort attempt to move Tail to our new node
    TaggedPtr desiredTail;
    desiredTail.ptr = newNode;
    desiredTail.version = localTail.version + 1;

    CAS(&Q.tail, localTail, desiredTail);

    return true;
}

// Returns true if dequeue succeeded.
// 'value' contains the dequeued element.
bool dequeue(string &value) {

    while (true) {

        // E1: Fetch head, tail and head->next
        TaggedPtr localHead = Q.head.load();
        TaggedPtr localTail = Q.tail.load();
        TaggedPtr localHeadNext = localHead.ptr->next.load();

        // E2: Check if head is still consistent
        if (localHead == Q.head.load()) {

            // E3: Queue empty or Tail is falling behind
            if (localHead.ptr == localTail.ptr) {

                // Queue is empty
                if (localHeadNext.ptr == nullptr) {
                    return false;
                }

                // Tail is behind, help move it forward
                TaggedPtr desiredTail;
                desiredTail.ptr = localHeadNext.ptr;
                desiredTail.version = localTail.version + 1;

                CAS(&Q.tail, localTail, desiredTail);
            }
            else {

                // E4: Read the value BEFORE doing the CAS
                value = localHeadNext.ptr->values;

                TaggedPtr desiredHead;
                desiredHead.ptr = localHeadNext.ptr;
                desiredHead.version = localHead.version + 1;

                // E5: Try moving head forward
                if (CAS(&Q.head,
                        localHead,
                        desiredHead)) {

                    // TODO:
                    // localHead.ptr is the old dummy node.
                    // Do NOT delete it unless using Hazard
                    // Pointers / Epoch Reclamation.

                    return true;
                }
            }
        }
    }
}

void producerFunc(int threadid) {
    // Mock for new IO arrives with some unique number
    for (int i = 1; i <= NUM_ITEMS_PER_PRODUCER; i++) {

        string temp = to_string(threadid) + "-" + to_string(i);

        // Push this in queue
        if (!enqueue(temp)) {
            // Something went wrong -> Take care of this exception
            continue;
        }

        cout << "Produced : " << temp << "\n";
    }
}

void consumerFunc(int threadid)
{
    while (true)
    {
        string value;

        if (dequeue(value))
        {
            cout << "Consumer " << threadid
                 << " consumed " << value << endl;
        }
        else
        {
            // Queue empty

            if (producersDone.load())
                break;

            // Give producers time to produce more
            this_thread::yield();
        }
    }
}

void producer() {
    // Produce the value provided
    // Multiple values generated by multiple threads
    // each thread will do the following:

    vector<thread> producerThreads;

    for (int i = 1; i <= NUM_PRODUCERS; i++) {
        producerThreads.emplace_back(producerFunc, i);
    }

    for (auto &t : producerThreads) {
        t.join();
    }
}

void consumer() {
    // Consume the value provided from the queue in any order and hand it over

    while (true) {

        string value;

        if (dequeue(value)) {
            cout << "Consumed : " << value << "\n";
        }
        else {
            // Queue empty
            break;
        }
    }
}

int main()
{
    init();

    vector<thread> producers;
    vector<thread> consumers;

    // Start producers
    for (int i = 0; i < NUM_PRODUCERS; i++)
        producers.emplace_back(producerFunc, i);

    // Start consumers
    for (int i = 0; i < NUM_PRODUCERS; i++)
        consumers.emplace_back(consumerFunc, i);

    // Wait for all producers
    for (auto &t : producers)
        t.join();

    // Tell consumers production is finished
    producersDone.store(true);

    // Wait for consumers
    for (auto &t : consumers)
        t.join();
}
