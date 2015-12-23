/*  =========================================================================
    raft_msg - raft over zyre

    Codec header for raft_msg.

    ** WARNING *************************************************************
    THIS SOURCE FILE IS 100% GENERATED. If you edit this file, you will lose
    your changes at the next build cycle. This is great for temporary printf
    statements. DO NOT MAKE ANY CHANGES YOU WISH TO KEEP. The correct places
    for commits are:

     * The XML model used for this code generation: raft_msg.xml, or
     * The code generation script that built this file: zproto_codec_mruby
    ************************************************************************
    =========================================================================
*/

#ifndef RAFT_MSG_H_INCLUDED
#define RAFT_MSG_H_INCLUDED

/*  These are the raft_msg messages:

    APPENDENTRY - Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
        term                number 8    leader's term
        prevlogindex        number 8    index of log entry immediately preceding new ones
        prevlogterm         number 8    term of prevLogIndex entry
        entry               chunk       log entry to store
        leadercommit        number 8    leader’s commitIndex

    APPENDENTRY_REPLY - 
        term                number 8    currentTerm, for leader to update itself
        success             number 1    1 if follower contained entry matching prevLogIndex and prevLogTerm

    REQUESTVOTE - Invoked by candidates to gather votes (§5.2).
        term                number 8    candidate’s term
        lastlogindex        number 8    index of candidate’s last log entry (§5.4)
        lastlogterm         number 8    term of candidate’s last log entry (§5.4)

    REQUESTVOTE_REPLY - 
        term                number 8    currentTerm, for candidate to update itself
        votegranted         number 1    1 means candidate received vote
*/


#define RAFT_MSG_APPENDENTRY                1
#define RAFT_MSG_APPENDENTRY_REPLY          2
#define RAFT_MSG_REQUESTVOTE                3
#define RAFT_MSG_REQUESTVOTE_REPLY          4

#include <czmq.h>

#ifdef __cplusplus
extern "C" {
#endif

//  Opaque class structure
#ifndef RAFT_MSG_T_DEFINED
typedef struct _raft_msg_t raft_msg_t;
#define RAFT_MSG_T_DEFINED
#endif

//  @interface
//  Create a new empty raft_msg
raft_msg_t *
    raft_msg_new (void);

//  Destroy a raft_msg instance
void
    raft_msg_destroy (raft_msg_t **self_p);

//  Deserialize a raft_msg from the specified message, popping
//  as many frames as needed. Returns 0 if OK, -1 if there was an error.
int
    raft_msg_recv (raft_msg_t *self, zmsg_t *input);

//  Serialize and append the raft_msg to the specified message
int
    raft_msg_send (raft_msg_t *self, zmsg_t *output);

//  Print contents of message to stdout
void
    raft_msg_print (raft_msg_t *self);

//  Get/set the message routing id
zframe_t *
    raft_msg_routing_id (raft_msg_t *self);
void
    raft_msg_set_routing_id (raft_msg_t *self, zframe_t *routing_id);

//  Get the raft_msg id and printable command
int
    raft_msg_id (raft_msg_t *self);
void
    raft_msg_set_id (raft_msg_t *self, int id);
const char *
    raft_msg_command (raft_msg_t *self);

//  Get/set the term field
uint64_t
    raft_msg_term (raft_msg_t *self);
void
    raft_msg_set_term (raft_msg_t *self, uint64_t term);

//  Get/set the prevlogindex field
uint64_t
    raft_msg_prevlogindex (raft_msg_t *self);
void
    raft_msg_set_prevlogindex (raft_msg_t *self, uint64_t prevlogindex);

//  Get/set the prevlogterm field
uint64_t
    raft_msg_prevlogterm (raft_msg_t *self);
void
    raft_msg_set_prevlogterm (raft_msg_t *self, uint64_t prevlogterm);

//  Get a copy of the entry field
zchunk_t *
    raft_msg_entry (raft_msg_t *self);
//  Get the entry field and transfer ownership to caller
zchunk_t *
    raft_msg_get_entry (raft_msg_t *self);
//  Set the entry field, transferring ownership from caller
void
    raft_msg_set_entry (raft_msg_t *self, zchunk_t **chunk_p);

//  Get/set the leadercommit field
uint64_t
    raft_msg_leadercommit (raft_msg_t *self);
void
    raft_msg_set_leadercommit (raft_msg_t *self, uint64_t leadercommit);

//  Get/set the success field
byte
    raft_msg_success (raft_msg_t *self);
void
    raft_msg_set_success (raft_msg_t *self, byte success);

//  Get/set the lastlogindex field
uint64_t
    raft_msg_lastlogindex (raft_msg_t *self);
void
    raft_msg_set_lastlogindex (raft_msg_t *self, uint64_t lastlogindex);

//  Get/set the lastlogterm field
uint64_t
    raft_msg_lastlogterm (raft_msg_t *self);
void
    raft_msg_set_lastlogterm (raft_msg_t *self, uint64_t lastlogterm);

//  Get/set the votegranted field
byte
    raft_msg_votegranted (raft_msg_t *self);
void
    raft_msg_set_votegranted (raft_msg_t *self, byte votegranted);

//  Self test of this class
int
    raft_msg_test (bool verbose);
//  @end

//  For backwards compatibility with old codecs
#define raft_msg_dump       raft_msg_print

#ifdef __cplusplus
}
#endif

#endif
