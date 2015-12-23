/*  =========================================================================
    raft_msg - raft over zyre

    Codec class for raft_msg.

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

/*
@header
    raft_msg - raft over zyre
@discuss
@end
*/

#include "./raft_msg.h"

//  Structure of our class

struct _raft_msg_t {
    zframe_t *routing_id;               //  Routing_id from ROUTER, if any
    int id;                             //  raft_msg message ID
    byte *needle;                       //  Read/write pointer for serialization
    byte *ceiling;                      //  Valid upper limit for read pointer
    uint64_t term;                      //  leader's term
    uint64_t prevlogindex;              //  index of log entry immediately preceding new ones
    uint64_t prevlogterm;               //  term of prevLogIndex entry
    zchunk_t *entry;                    //  log entry to store
    uint64_t leadercommit;              //  leader’s commitIndex
    byte success;                       //  1 if follower contained entry matching prevLogIndex and prevLogTerm
    uint64_t lastlogindex;              //  index of candidate’s last log entry (§5.4)
    uint64_t lastlogterm;               //  term of candidate’s last log entry (§5.4)
    byte votegranted;                   //  1 means candidate received vote
};

//  --------------------------------------------------------------------------
//  Network data encoding macros

//  Put a block of octets to the frame
#define PUT_OCTETS(host,size) { \
    memcpy (self->needle, (host), size); \
    self->needle += size; \
}

//  Get a block of octets from the frame
#define GET_OCTETS(host,size) { \
    if (self->needle + size > self->ceiling) { \
        zsys_warning ("raft_msg: GET_OCTETS failed"); \
        goto malformed; \
    } \
    memcpy ((host), self->needle, size); \
    self->needle += size; \
}

//  Put a 1-byte number to the frame
#define PUT_NUMBER1(host) { \
    *(byte *) self->needle = (host); \
    self->needle++; \
}

//  Put a 2-byte number to the frame
#define PUT_NUMBER2(host) { \
    self->needle [0] = (byte) (((host) >> 8)  & 255); \
    self->needle [1] = (byte) (((host))       & 255); \
    self->needle += 2; \
}

//  Put a 4-byte number to the frame
#define PUT_NUMBER4(host) { \
    self->needle [0] = (byte) (((host) >> 24) & 255); \
    self->needle [1] = (byte) (((host) >> 16) & 255); \
    self->needle [2] = (byte) (((host) >> 8)  & 255); \
    self->needle [3] = (byte) (((host))       & 255); \
    self->needle += 4; \
}

//  Put a 8-byte number to the frame
#define PUT_NUMBER8(host) { \
    self->needle [0] = (byte) (((host) >> 56) & 255); \
    self->needle [1] = (byte) (((host) >> 48) & 255); \
    self->needle [2] = (byte) (((host) >> 40) & 255); \
    self->needle [3] = (byte) (((host) >> 32) & 255); \
    self->needle [4] = (byte) (((host) >> 24) & 255); \
    self->needle [5] = (byte) (((host) >> 16) & 255); \
    self->needle [6] = (byte) (((host) >> 8)  & 255); \
    self->needle [7] = (byte) (((host))       & 255); \
    self->needle += 8; \
}

//  Get a 1-byte number from the frame
#define GET_NUMBER1(host) { \
    if (self->needle + 1 > self->ceiling) { \
        zsys_warning ("raft_msg: GET_NUMBER1 failed"); \
        goto malformed; \
    } \
    (host) = *(byte *) self->needle; \
    self->needle++; \
}

//  Get a 2-byte number from the frame
#define GET_NUMBER2(host) { \
    if (self->needle + 2 > self->ceiling) { \
        zsys_warning ("raft_msg: GET_NUMBER2 failed"); \
        goto malformed; \
    } \
    (host) = ((uint16_t) (self->needle [0]) << 8) \
           +  (uint16_t) (self->needle [1]); \
    self->needle += 2; \
}

//  Get a 4-byte number from the frame
#define GET_NUMBER4(host) { \
    if (self->needle + 4 > self->ceiling) { \
        zsys_warning ("raft_msg: GET_NUMBER4 failed"); \
        goto malformed; \
    } \
    (host) = ((uint32_t) (self->needle [0]) << 24) \
           + ((uint32_t) (self->needle [1]) << 16) \
           + ((uint32_t) (self->needle [2]) << 8) \
           +  (uint32_t) (self->needle [3]); \
    self->needle += 4; \
}

//  Get a 8-byte number from the frame
#define GET_NUMBER8(host) { \
    if (self->needle + 8 > self->ceiling) { \
        zsys_warning ("raft_msg: GET_NUMBER8 failed"); \
        goto malformed; \
    } \
    (host) = ((uint64_t) (self->needle [0]) << 56) \
           + ((uint64_t) (self->needle [1]) << 48) \
           + ((uint64_t) (self->needle [2]) << 40) \
           + ((uint64_t) (self->needle [3]) << 32) \
           + ((uint64_t) (self->needle [4]) << 24) \
           + ((uint64_t) (self->needle [5]) << 16) \
           + ((uint64_t) (self->needle [6]) << 8) \
           +  (uint64_t) (self->needle [7]); \
    self->needle += 8; \
}

//  Put a string to the frame
#define PUT_STRING(host) { \
    size_t string_size = strlen (host); \
    PUT_NUMBER1 (string_size); \
    memcpy (self->needle, (host), string_size); \
    self->needle += string_size; \
}

//  Get a string from the frame
#define GET_STRING(host) { \
    size_t string_size; \
    GET_NUMBER1 (string_size); \
    if (self->needle + string_size > (self->ceiling)) { \
        zsys_warning ("raft_msg: GET_STRING failed"); \
        goto malformed; \
    } \
    memcpy ((host), self->needle, string_size); \
    (host) [string_size] = 0; \
    self->needle += string_size; \
}

//  Put a long string to the frame
#define PUT_LONGSTR(host) { \
    size_t string_size = strlen (host); \
    PUT_NUMBER4 (string_size); \
    memcpy (self->needle, (host), string_size); \
    self->needle += string_size; \
}

//  Get a long string from the frame
#define GET_LONGSTR(host) { \
    size_t string_size; \
    GET_NUMBER4 (string_size); \
    if (self->needle + string_size > (self->ceiling)) { \
        zsys_warning ("raft_msg: GET_LONGSTR failed"); \
        goto malformed; \
    } \
    free ((host)); \
    (host) = (char *) malloc (string_size + 1); \
    memcpy ((host), self->needle, string_size); \
    (host) [string_size] = 0; \
    self->needle += string_size; \
}


//  --------------------------------------------------------------------------
//  Create a new raft_msg

raft_msg_t *
raft_msg_new (void)
{
    raft_msg_t *self = (raft_msg_t *) zmalloc (sizeof (raft_msg_t));
    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the raft_msg

void
raft_msg_destroy (raft_msg_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        raft_msg_t *self = *self_p;

        //  Free class properties
        zframe_destroy (&self->routing_id);
        zchunk_destroy (&self->entry);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}


//  --------------------------------------------------------------------------
//  Deserialize a raft_msg from the specified message, popping
//  as many frames as needed. Returns 0 if OK, -1 if there was an error.
int
raft_msg_recv (raft_msg_t *self, zmsg_t *input)
{
    assert (input);
    zframe_t *frame = zmsg_pop (input);
    if (!frame) {
        zsys_warning ("raft_msg: missing frames in message");
        goto malformed;         //  Interrupted
    }
    //  Get and check protocol signature
    self->needle = zframe_data (frame);
    self->ceiling = self->needle + zframe_size (frame);

    uint16_t signature;
    GET_NUMBER2 (signature);
    if (signature != (0xAAA0 | 6)) {
        zsys_warning ("raft_msg: invalid signature");
        goto malformed;         //  Interrupted
    }
    //  Get message id and parse per message type
    GET_NUMBER1 (self->id);

    switch (self->id) {
        case RAFT_MSG_APPENDENTRY:
            GET_NUMBER8 (self->term);
            GET_NUMBER8 (self->prevlogindex);
            GET_NUMBER8 (self->prevlogterm);
            {
                size_t chunk_size;
                GET_NUMBER4 (chunk_size);
                if (self->needle + chunk_size > (self->ceiling)) {
                    zsys_warning ("raft_msg: entry is missing data");
                    goto malformed;
                }
                zchunk_destroy (&self->entry);
                self->entry = zchunk_new (self->needle, chunk_size);
                self->needle += chunk_size;
            }
            GET_NUMBER8 (self->leadercommit);
            break;

        case RAFT_MSG_APPENDENTRY_REPLY:
            GET_NUMBER8 (self->term);
            GET_NUMBER1 (self->success);
            break;

        case RAFT_MSG_REQUESTVOTE:
            GET_NUMBER8 (self->term);
            GET_NUMBER8 (self->lastlogindex);
            GET_NUMBER8 (self->lastlogterm);
            break;

        case RAFT_MSG_REQUESTVOTE_REPLY:
            GET_NUMBER8 (self->term);
            GET_NUMBER1 (self->votegranted);
            break;

        default:
            zsys_warning ("raft_msg: bad message ID");
            goto malformed;
    }
    zframe_destroy (&frame);
    //  Successful return
    return 0;

    //  Error returns
    malformed:
        zframe_destroy (&frame);
        zsys_warning ("raft_msg: raft_msg malformed message, fail");
        return -1;              //  Invalid message
}


//  --------------------------------------------------------------------------
//  Serialize and append the raft_msg to the specified message
int
raft_msg_send (raft_msg_t *self, zmsg_t *output)
{
    assert (self);
    assert (output);
    size_t frame_size = 2 + 1;          //  Signature and message ID
    switch (self->id) {
        case RAFT_MSG_APPENDENTRY:
            frame_size += 8;            //  term
            frame_size += 8;            //  prevlogindex
            frame_size += 8;            //  prevlogterm
            frame_size += 4;            //  Size is 4 octets
            if (self->entry)
                frame_size += zchunk_size (self->entry);
            frame_size += 8;            //  leadercommit
            break;
        case RAFT_MSG_APPENDENTRY_REPLY:
            frame_size += 8;            //  term
            frame_size += 1;            //  success
            break;
        case RAFT_MSG_REQUESTVOTE:
            frame_size += 8;            //  term
            frame_size += 8;            //  lastlogindex
            frame_size += 8;            //  lastlogterm
            break;
        case RAFT_MSG_REQUESTVOTE_REPLY:
            frame_size += 8;            //  term
            frame_size += 1;            //  votegranted
            break;
    }
    //  Now serialize message into the frame
    zframe_t *frame = zframe_new (NULL, frame_size);
    self->needle = zframe_data (frame);
    PUT_NUMBER2 (0xAAA0 | 6);
    PUT_NUMBER1 (self->id);

    switch (self->id) {
        case RAFT_MSG_APPENDENTRY:
            PUT_NUMBER8 (self->term);
            PUT_NUMBER8 (self->prevlogindex);
            PUT_NUMBER8 (self->prevlogterm);
            if (self->entry) {
                PUT_NUMBER4 (zchunk_size (self->entry));
                memcpy (self->needle,
                        zchunk_data (self->entry),
                        zchunk_size (self->entry));
                self->needle += zchunk_size (self->entry);
            }
            else
                PUT_NUMBER4 (0);    //  Empty chunk
            PUT_NUMBER8 (self->leadercommit);
            break;

        case RAFT_MSG_APPENDENTRY_REPLY:
            PUT_NUMBER8 (self->term);
            PUT_NUMBER1 (self->success);
            break;

        case RAFT_MSG_REQUESTVOTE:
            PUT_NUMBER8 (self->term);
            PUT_NUMBER8 (self->lastlogindex);
            PUT_NUMBER8 (self->lastlogterm);
            break;

        case RAFT_MSG_REQUESTVOTE_REPLY:
            PUT_NUMBER8 (self->term);
            PUT_NUMBER1 (self->votegranted);
            break;

    }
    //  Now store the frame data
    zmsg_append (output, &frame);

    return 0;
}


//  --------------------------------------------------------------------------
//  Print contents of message to stdout

void
raft_msg_print (raft_msg_t *self)
{
    assert (self);
    switch (self->id) {
        case RAFT_MSG_APPENDENTRY:
            zsys_debug ("RAFT_MSG_APPENDENTRY:");
            zsys_debug ("    term=%ld", (long) self->term);
            zsys_debug ("    prevlogindex=%ld", (long) self->prevlogindex);
            zsys_debug ("    prevlogterm=%ld", (long) self->prevlogterm);
            zsys_debug ("    entry=[ ... ]");
            zsys_debug ("    leadercommit=%ld", (long) self->leadercommit);
            break;

        case RAFT_MSG_APPENDENTRY_REPLY:
            zsys_debug ("RAFT_MSG_APPENDENTRY_REPLY:");
            zsys_debug ("    term=%ld", (long) self->term);
            zsys_debug ("    success=%ld", (long) self->success);
            break;

        case RAFT_MSG_REQUESTVOTE:
            zsys_debug ("RAFT_MSG_REQUESTVOTE:");
            zsys_debug ("    term=%ld", (long) self->term);
            zsys_debug ("    lastlogindex=%ld", (long) self->lastlogindex);
            zsys_debug ("    lastlogterm=%ld", (long) self->lastlogterm);
            break;

        case RAFT_MSG_REQUESTVOTE_REPLY:
            zsys_debug ("RAFT_MSG_REQUESTVOTE_REPLY:");
            zsys_debug ("    term=%ld", (long) self->term);
            zsys_debug ("    votegranted=%ld", (long) self->votegranted);
            break;

    }
}


//  --------------------------------------------------------------------------
//  Get/set the message routing_id

zframe_t *
raft_msg_routing_id (raft_msg_t *self)
{
    assert (self);
    return self->routing_id;
}

void
raft_msg_set_routing_id (raft_msg_t *self, zframe_t *routing_id)
{
    if (self->routing_id)
        zframe_destroy (&self->routing_id);
    self->routing_id = zframe_dup (routing_id);
}


//  --------------------------------------------------------------------------
//  Get/set the raft_msg id

int
raft_msg_id (raft_msg_t *self)
{
    assert (self);
    return self->id;
}

void
raft_msg_set_id (raft_msg_t *self, int id)
{
    self->id = id;
}

//  --------------------------------------------------------------------------
//  Return a printable command string

const char *
raft_msg_command (raft_msg_t *self)
{
    assert (self);
    switch (self->id) {
        case RAFT_MSG_APPENDENTRY:
            return ("APPENDENTRY");
            break;
        case RAFT_MSG_APPENDENTRY_REPLY:
            return ("APPENDENTRY_REPLY");
            break;
        case RAFT_MSG_REQUESTVOTE:
            return ("REQUESTVOTE");
            break;
        case RAFT_MSG_REQUESTVOTE_REPLY:
            return ("REQUESTVOTE_REPLY");
            break;
    }
    return "?";
}

//  --------------------------------------------------------------------------
//  Get/set the term field

uint64_t
raft_msg_term (raft_msg_t *self)
{
    assert (self);
    return self->term;
}

void
raft_msg_set_term (raft_msg_t *self, uint64_t term)
{
    assert (self);
    self->term = term;
}


//  --------------------------------------------------------------------------
//  Get/set the prevlogindex field

uint64_t
raft_msg_prevlogindex (raft_msg_t *self)
{
    assert (self);
    return self->prevlogindex;
}

void
raft_msg_set_prevlogindex (raft_msg_t *self, uint64_t prevlogindex)
{
    assert (self);
    self->prevlogindex = prevlogindex;
}


//  --------------------------------------------------------------------------
//  Get/set the prevlogterm field

uint64_t
raft_msg_prevlogterm (raft_msg_t *self)
{
    assert (self);
    return self->prevlogterm;
}

void
raft_msg_set_prevlogterm (raft_msg_t *self, uint64_t prevlogterm)
{
    assert (self);
    self->prevlogterm = prevlogterm;
}


//  --------------------------------------------------------------------------
//  Get the entry field without transferring ownership

zchunk_t *
raft_msg_entry (raft_msg_t *self)
{
    assert (self);
    return self->entry;
}

//  Get the entry field and transfer ownership to caller

zchunk_t *
raft_msg_get_entry (raft_msg_t *self)
{
    zchunk_t *entry = self->entry;
    self->entry = NULL;
    return entry;
}

//  Set the entry field, transferring ownership from caller

void
raft_msg_set_entry (raft_msg_t *self, zchunk_t **chunk_p)
{
    assert (self);
    assert (chunk_p);
    zchunk_destroy (&self->entry);
    self->entry = *chunk_p;
    *chunk_p = NULL;
}


//  --------------------------------------------------------------------------
//  Get/set the leadercommit field

uint64_t
raft_msg_leadercommit (raft_msg_t *self)
{
    assert (self);
    return self->leadercommit;
}

void
raft_msg_set_leadercommit (raft_msg_t *self, uint64_t leadercommit)
{
    assert (self);
    self->leadercommit = leadercommit;
}


//  --------------------------------------------------------------------------
//  Get/set the success field

byte
raft_msg_success (raft_msg_t *self)
{
    assert (self);
    return self->success;
}

void
raft_msg_set_success (raft_msg_t *self, byte success)
{
    assert (self);
    self->success = success;
}


//  --------------------------------------------------------------------------
//  Get/set the lastlogindex field

uint64_t
raft_msg_lastlogindex (raft_msg_t *self)
{
    assert (self);
    return self->lastlogindex;
}

void
raft_msg_set_lastlogindex (raft_msg_t *self, uint64_t lastlogindex)
{
    assert (self);
    self->lastlogindex = lastlogindex;
}


//  --------------------------------------------------------------------------
//  Get/set the lastlogterm field

uint64_t
raft_msg_lastlogterm (raft_msg_t *self)
{
    assert (self);
    return self->lastlogterm;
}

void
raft_msg_set_lastlogterm (raft_msg_t *self, uint64_t lastlogterm)
{
    assert (self);
    self->lastlogterm = lastlogterm;
}


//  --------------------------------------------------------------------------
//  Get/set the votegranted field

byte
raft_msg_votegranted (raft_msg_t *self)
{
    assert (self);
    return self->votegranted;
}

void
raft_msg_set_votegranted (raft_msg_t *self, byte votegranted)
{
    assert (self);
    self->votegranted = votegranted;
}



//  --------------------------------------------------------------------------
//  Selftest

int
raft_msg_test (bool verbose)
{
    printf (" * raft_msg:");

    if (verbose)
        printf ("\n");

    //  @selftest
    //  Simple create/destroy test
    raft_msg_t *self = raft_msg_new ();
    assert (self);
    raft_msg_destroy (&self);
    zmsg_t *output = zmsg_new ();
    assert (output);

    zmsg_t *input = zmsg_new ();
    assert (input);


    //  Encode/send/decode and verify each message type
    int instance;
    self = raft_msg_new ();
    raft_msg_set_id (self, RAFT_MSG_APPENDENTRY);

    raft_msg_set_term (self, 123);
    raft_msg_set_prevlogindex (self, 123);
    raft_msg_set_prevlogterm (self, 123);
    zchunk_t *appendentry_entry = zchunk_new ("Captcha Diem", 12);
    raft_msg_set_entry (self, &appendentry_entry);
    raft_msg_set_leadercommit (self, 123);
    zmsg_destroy (&output);
    output = zmsg_new ();
    assert (output);
    //  Send twice
    raft_msg_send (self, output);
    raft_msg_send (self, output);

    zmsg_destroy (&input);
    input = zmsg_dup (output);
    assert (input);
    for (instance = 0; instance < 2; instance++) {
        raft_msg_recv (self, input);
        assert (raft_msg_routing_id (self) == NULL);
        assert (raft_msg_term (self) == 123);
        assert (raft_msg_prevlogindex (self) == 123);
        assert (raft_msg_prevlogterm (self) == 123);
        assert (memcmp (zchunk_data (raft_msg_entry (self)), "Captcha Diem", 12) == 0);
        if (instance == 1)
            zchunk_destroy (&appendentry_entry);
        assert (raft_msg_leadercommit (self) == 123);
    }
    raft_msg_set_id (self, RAFT_MSG_APPENDENTRY_REPLY);

    raft_msg_set_term (self, 123);
    raft_msg_set_success (self, 123);
    zmsg_destroy (&output);
    output = zmsg_new ();
    assert (output);
    //  Send twice
    raft_msg_send (self, output);
    raft_msg_send (self, output);

    zmsg_destroy (&input);
    input = zmsg_dup (output);
    assert (input);
    for (instance = 0; instance < 2; instance++) {
        raft_msg_recv (self, input);
        assert (raft_msg_routing_id (self) == NULL);
        assert (raft_msg_term (self) == 123);
        assert (raft_msg_success (self) == 123);
    }
    raft_msg_set_id (self, RAFT_MSG_REQUESTVOTE);

    raft_msg_set_term (self, 123);
    raft_msg_set_lastlogindex (self, 123);
    raft_msg_set_lastlogterm (self, 123);
    zmsg_destroy (&output);
    output = zmsg_new ();
    assert (output);
    //  Send twice
    raft_msg_send (self, output);
    raft_msg_send (self, output);

    zmsg_destroy (&input);
    input = zmsg_dup (output);
    assert (input);
    for (instance = 0; instance < 2; instance++) {
        raft_msg_recv (self, input);
        assert (raft_msg_routing_id (self) == NULL);
        assert (raft_msg_term (self) == 123);
        assert (raft_msg_lastlogindex (self) == 123);
        assert (raft_msg_lastlogterm (self) == 123);
    }
    raft_msg_set_id (self, RAFT_MSG_REQUESTVOTE_REPLY);

    raft_msg_set_term (self, 123);
    raft_msg_set_votegranted (self, 123);
    zmsg_destroy (&output);
    output = zmsg_new ();
    assert (output);
    //  Send twice
    raft_msg_send (self, output);
    raft_msg_send (self, output);

    zmsg_destroy (&input);
    input = zmsg_dup (output);
    assert (input);
    for (instance = 0; instance < 2; instance++) {
        raft_msg_recv (self, input);
        assert (raft_msg_routing_id (self) == NULL);
        assert (raft_msg_term (self) == 123);
        assert (raft_msg_votegranted (self) == 123);
    }

    raft_msg_destroy (&self);
    zmsg_destroy (&input);
    zmsg_destroy (&output);
    //  @end

    printf ("OK\n");
    return 0;
}
