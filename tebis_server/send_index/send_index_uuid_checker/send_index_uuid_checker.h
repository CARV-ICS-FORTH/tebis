#ifndef SEND_INDEX_UUID_CHECKER_H
#define SEND_INDEX_UUID_CHECKER_H
#include "../../messages.h"
#include "../../metadata.h"

/**
 *  @brief Given that the uuid field is piggybacked and initialized for both request and reply, the function validates that the uuids are equal for the server-to-server communication
 * @param msg_pair: an sc_msg_pair ptr which contains the request and the reply from which the uuid field will be checked
 * @param request_msg_type: The msg_type of the request (e.g. CLOSE_COMPACTION_REQUEST)
 */
void send_index_uuid_checker_validate_uuid(struct sc_msg_pair *msg_pair, enum message_type request_msg_type);

#endif // SEND_INDEX_UUID_CHECKER_H
