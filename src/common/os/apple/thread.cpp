#include "../thread.h"
#include "../../utf.h"
#include <pthread.h>
#include <pthread/qos.h>

namespace caspar {

void set_thread_name(const std::wstring& name) { pthread_setname_np(u8(name).c_str()); }

// macOS takes SCHED_FIFO's sched_priority as the thread's absolute Mach
// priority, so the Linux version's 2 puts the thread below even background
// work (4) and it starves under load. User-interactive QoS is the highest a
// thread gets without the time-constraint policy.
void set_thread_realtime_priority() { pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0); }

} // namespace caspar
