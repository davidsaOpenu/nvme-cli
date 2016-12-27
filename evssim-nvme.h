#undef CMD_INC_FILE
#define CMD_INC_FILE evssim-nvme

#if !defined(EVSSIM_NVME) || defined(CMD_HEADER_MULTI_READ)
#define EVSSIM_NVME

#include "cmd.h"

PLUGIN(NAME("evssim", "Evssim object ssd handling extension"),
    COMMAND_LIST(
        ENTRY("create", "Create a new object", create_object)
        ENTRY("delete", "Delete an existing object", delete_object)
        ENTRY("write", "Write to a given object", write_object)
        ENTRY("read", "Read from a given object", read_object)
    )
);

#endif

#include "define_cmd.h"