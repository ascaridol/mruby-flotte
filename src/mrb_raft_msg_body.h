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
#include <errno.h>
#include <mruby.h>
#include <mruby/array.h>
#include <mruby/class.h>
#include <mruby/data.h>
#include <mruby/error.h>
#include <mruby/hash.h>
#include <mruby/string.h>
#include <mruby/throw.h>

static void
mrb_raft_msg_destroy (mrb_state *mrb, void *p)
{
    raft_msg_destroy ((raft_msg_t **) &p);
}

static const struct mrb_data_type mrb_raft_msg_type = {
    "i_raft_msg_type", mrb_raft_msg_destroy
};

//  --------------------------------------------------------------------------
//  Create a new raft_msg

static mrb_value
mrb_raft_msg_initialize (mrb_state *mrb, mrb_value mrb_self)
{
    errno = 0;
    raft_msg_t *self = raft_msg_new ();
    if (self != NULL) {
        mrb_data_init (mrb_self, self, &mrb_raft_msg_type);
    } else {
        mrb_sys_fail (mrb, "raft_msg_new");
    }

    return mrb_self;
}

static inline void *
mrb_zproto_codec_get_ptr (mrb_state *mrb, mrb_value ptr_obj)
{
    switch (mrb_type (ptr_obj)) {
        case MRB_TT_DATA:
            return DATA_PTR (ptr_obj);
            break;
        case MRB_TT_CPTR:
            return mrb_cptr (ptr_obj);
            break;
        default:
            mrb_raise(mrb, E_ARGUMENT_ERROR, "is not a data or cptr type");
    }
}

//  --------------------------------------------------------------------------
//  Receive a raft_msg from the socket. Returns self if OK, else raises an exception.
//  Blocks if there is no message waiting.

static mrb_value
mrb_raft_msg_recv (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_value input_obj;

    mrb_get_args (mrb, "o", &input_obj);

    zmsg_t *input = (zmsg_t *) mrb_zproto_codec_get_ptr (mrb, input_obj);
    mrb_assert (zmsg_is (input));

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    errno = 0;
    if (raft_msg_recv (self, input) == -1) {
        mrb_sys_fail (mrb, "raft_msg_recv");
    }

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Send the raft_msg to the socket. Does not destroy it. Returns self if
//  OK, else raises an exception.

static mrb_value
mrb_raft_msg_send (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_value output_obj;

    mrb_get_args (mrb, "o", &output_obj);

    zmsg_t *output = (zmsg_t *) mrb_zproto_codec_get_ptr (mrb, output_obj);
    mrb_assert (zmsg_is (output_obj));

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    errno = 0;
    if (raft_msg_send (self, output) == -1) {
        mrb_sys_fail (mrb, "raft_msg_send");
    }

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Print contents of message to stdout

static mrb_value
mrb_raft_msg_print (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_print (self);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the message routing_id

static mrb_value
mrb_raft_msg_routing_id (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    zframe_t *routing_id = raft_msg_routing_id (self);

    return mrb_str_new_static (mrb, (const char *) zframe_data (routing_id), zframe_size (routing_id));
}

//  Set the message routing_id

static mrb_value
mrb_raft_msg_set_routing_id (mrb_state *mrb, mrb_value mrb_self)
{
    char *routing_id_str;
    mrb_int routing_id_len;

    mrb_get_args (mrb, "s", &routing_id_str, &routing_id_len);

    if (routing_id_len > 255) {
        mrb_raise (mrb, E_RANGE_ERROR, "routing_id is too large");
    }

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    zframe_t *routing_id = zframe_new (routing_id_str, routing_id_len);

    raft_msg_set_routing_id (self, routing_id);

    zframe_destroy (&routing_id);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the raft_msg id

static mrb_value
mrb_raft_msg_id (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_id (self));
}

// Set the raft_msg id

static mrb_value
mrb_raft_msg_set_id (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int id;

    mrb_get_args (mrb, "i", &id);

    if (id < INT_MIN || id > INT_MAX)
        mrb_raise (mrb, E_RANGE_ERROR, "id is out of range");

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_id (self, id);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Return a printable command string

static mrb_value
mrb_raft_msg_command (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    const char *command = raft_msg_command (self);

    return mrb_str_new_static (mrb, command, strlen (command));
}

//  --------------------------------------------------------------------------
//  Get the term field

static mrb_value
mrb_raft_msg_term (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_term (self));
}

// Set the term field

static mrb_value
mrb_raft_msg_set_term (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int term;

    mrb_get_args (mrb, "i", &term);

    // TODO: add bounds checking

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_term (self, term);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the prevlogindex field

static mrb_value
mrb_raft_msg_prevlogindex (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_prevlogindex (self));
}

// Set the prevlogindex field

static mrb_value
mrb_raft_msg_set_prevlogindex (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int prevlogindex;

    mrb_get_args (mrb, "i", &prevlogindex);

    // TODO: add bounds checking

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_prevlogindex (self, prevlogindex);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the prevlogterm field

static mrb_value
mrb_raft_msg_prevlogterm (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_prevlogterm (self));
}

// Set the prevlogterm field

static mrb_value
mrb_raft_msg_set_prevlogterm (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int prevlogterm;

    mrb_get_args (mrb, "i", &prevlogterm);

    // TODO: add bounds checking

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_prevlogterm (self, prevlogterm);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the entry field

static mrb_value
mrb_raft_msg_entry (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    zchunk_t *entry = raft_msg_entry (self);

    return mrb_str_new_static (mrb, (const char *) zchunk_data (entry), zchunk_size (entry));
}

//  Set the entry field

static mrb_value
mrb_raft_msg_set_entry (mrb_state *mrb, mrb_value mrb_self)
{
    char *entry_str;
    mrb_int entry_len;

    mrb_get_args (mrb, "s", &entry_str, &entry_len);

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    zchunk_t *entry = zchunk_new (entry_str, entry_len);

    raft_msg_set_entry (self, &entry);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the leadercommit field

static mrb_value
mrb_raft_msg_leadercommit (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_leadercommit (self));
}

// Set the leadercommit field

static mrb_value
mrb_raft_msg_set_leadercommit (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int leadercommit;

    mrb_get_args (mrb, "i", &leadercommit);

    // TODO: add bounds checking

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_leadercommit (self, leadercommit);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the success field

static mrb_value
mrb_raft_msg_success (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_success (self));
}

// Set the success field

static mrb_value
mrb_raft_msg_set_success (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int success;

    mrb_get_args (mrb, "i", &success);

    // TODO: add bounds checking

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_success (self, success);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the lastlogindex field

static mrb_value
mrb_raft_msg_lastlogindex (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_lastlogindex (self));
}

// Set the lastlogindex field

static mrb_value
mrb_raft_msg_set_lastlogindex (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int lastlogindex;

    mrb_get_args (mrb, "i", &lastlogindex);

    // TODO: add bounds checking

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_lastlogindex (self, lastlogindex);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the lastlogterm field

static mrb_value
mrb_raft_msg_lastlogterm (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_lastlogterm (self));
}

// Set the lastlogterm field

static mrb_value
mrb_raft_msg_set_lastlogterm (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int lastlogterm;

    mrb_get_args (mrb, "i", &lastlogterm);

    // TODO: add bounds checking

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_lastlogterm (self, lastlogterm);

    return mrb_self;
}

//  --------------------------------------------------------------------------
//  Get the votegranted field

static mrb_value
mrb_raft_msg_votegranted (mrb_state *mrb, mrb_value mrb_self)
{
    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    // TODO: add bounds checking

    return mrb_fixnum_value (raft_msg_votegranted (self));
}

// Set the votegranted field

static mrb_value
mrb_raft_msg_set_votegranted (mrb_state *mrb, mrb_value mrb_self)
{
    mrb_int votegranted;

    mrb_get_args (mrb, "i", &votegranted);

    // TODO: add bounds checking

    raft_msg_t *self = (raft_msg_t *) DATA_PTR (mrb_self);

    raft_msg_set_votegranted (self, votegranted);

    return mrb_self;
}

