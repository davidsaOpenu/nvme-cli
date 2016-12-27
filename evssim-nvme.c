#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <linux/fs.h>

#include <unistd.h>
#include <fcntl.h>

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include "nvme.h"
#include "nvme-ioctl.h"
#include "nvme-print.h"

#include "plugin.h"

#define CREATE_CMD
#include "evssim-nvme.h"

struct nvme_object_io {
	__u8	opcode;
	__u8	flags;
	__u16	control;
	__u16	size;
	__u16	rsvd;
	__u64	metadata;
	__u64	addr;
	__u64	object_id;
	__u32	dsmgmt;
	__u32	reftag;
	__u16	apptag;
	__u16	appmask;
};

struct nvme_object_ctrl {
	__u8	opcode;
	__u8	flags;
	__u64	object_id;
	__u64	metadata;
	__u64	length;
	__u32	nsid;
};


#define NVME_IOCTL_OBJECT_SUBMIT_IO	_IOWR('N', 0x50, struct nvme_object_io)
#define NVME_IOCTL_OBJECT_CTRL   	_IOWR('N', 0x51, struct nvme_object_ctrl)

int nvme_submit_object_ctrl(int fd, struct nvme_object_ctrl * ctrl)
{
	return ioctl(fd, NVME_IOCTL_OBJECT_CTRL, ctrl);
}

static int create_object(int argc, char **argv, struct command *cmd, struct plugin *plugin)
{
	const char *desc = "Send a command to the given"\
		"device to create a new object and report "\
		"the created object's id.";

	struct config {};
	struct config cfg = {};

	const struct argconfig_commandline_options command_line_options[] = {
		{0}
	};

	int fd = parse_and_open(argc, argv, desc, command_line_options, &cfg, sizeof(cfg));

	struct nvme_object_ctrl obj_ctrl = {
		.opcode = nvme_cmd_obj_create,
		.nsid	= 1,
	};

	int status = nvme_submit_object_ctrl(fd, &obj_ctrl);
	if (!status)
		printf("object id = 0x%"PRIx64"\n", (uint64_t)obj_ctrl.object_id);

	return status;
}

static int delete_object(int argc, char **argv, struct command *cmd, struct plugin *plugin)
{
	const char *desc = "Send a command to the given"\
		"device to delete the object of the given"\
		"id. ";

	const char *object = "ID of object to delete from namespace";

	struct config {
		__u64 object_id;
	};
	struct config cfg = {};

	const struct argconfig_commandline_options command_line_options[] = {
		{"object-id", 'n', "NUM", CFG_LONG_SUFFIX, &cfg.object_id, required_argument, object},
		{0}
	};

	int fd = parse_and_open(argc, argv, desc, command_line_options, &cfg, sizeof(cfg));

	struct nvme_object_ctrl obj_ctrl = {
		.opcode 	= nvme_cmd_obj_delete,
		.nsid		= 1,
		.object_id 	= cfg.object_id,
	};

	return nvme_submit_object_ctrl(fd, &obj_ctrl);
}

static int ceiling(int num, int mod)
{
	int rem = num % mod;
	return rem ? (num + mod - rem) : num;
}

static int submit_object_io(int opcode, char *command, const char *desc, int argc, char **argv)
{
	struct timeval start_time, end_time;
	void *buffer, *mbuffer = NULL;
	int err = 0;
	int dfd, mfd;
	int flags = opcode & 1 ? O_RDONLY : O_WRONLY | O_CREAT;
	int mode = S_IRUSR | S_IWUSR |S_IRGRP | S_IWGRP| S_IROTH;
	__u16 control = 0;
	int phys_sector_size = 0;
	long long buffer_size = 0;

	const char *object_id = "64-bit id of the object to write to";
	const char *data_size = "size of data in bytes";
	const char *metadata_size = "size of metadata in bytes";
	const char *ref_tag = "reference tag (for end to end PI)";
	const char *data = "data file";
	const char *metadata = "metadata file";
	const char *prinfo = "PI and check field";
	const char *app_tag_mask = "app tag mask (for end to end PI)";
	const char *app_tag = "app tag (for end to end PI)";
	const char *limited_retry = "limit num. media access attempts";
	const char *latency = "output latency statistics";
	const char *force = "force device to commit data before command completes";
	const char *show = "show command before sending";
	const char *dry = "show command instead of sending";

	struct config {
		__u64 object_id;
		__u64 data_size;
		__u64 metadata_size;
		__u32 ref_tag;
		char  *data;
		char  *metadata;
		__u8  prinfo;
		__u8  app_tag_mask;
		__u32 app_tag;
		int   limited_retry;
		int   force_unit_access;
		int   show;
		int   dry_run;
		int   latency;
	};

	struct config cfg = {
		.object_id       = 0,
		.data_size       = 0,
		.metadata_size   = 0,
		.ref_tag         = 0,
		.data            = "",
		.metadata        = "",
		.prinfo          = 0,
		.app_tag_mask    = 0,
		.app_tag         = 0,
	};

	const struct argconfig_commandline_options command_line_options[] = {
		{"object-id",         's', "NUM",  CFG_LONG_SUFFIX, &cfg.object_id,         required_argument, object_id},
		{"data-size",         'z', "NUM",  CFG_LONG_SUFFIX, &cfg.data_size,         required_argument, data_size},
		{"metadata-size",     'y', "NUM",  CFG_LONG_SUFFIX, &cfg.metadata_size,     required_argument, metadata_size},
		{"ref-tag",           'r', "NUM",  CFG_POSITIVE,    &cfg.ref_tag,           required_argument, ref_tag},
		{"data",              'd', "FILE", CFG_STRING,      &cfg.data,              required_argument, data},
		{"metadata",          'M', "FILE", CFG_STRING,      &cfg.metadata,          required_argument, metadata},
		{"prinfo",            'p', "NUM",  CFG_BYTE,        &cfg.prinfo,            required_argument, prinfo},
		{"app-tag-mask",      'm', "NUM",  CFG_BYTE,        &cfg.app_tag_mask,      required_argument, app_tag_mask},
		{"app-tag",           'a', "NUM",  CFG_POSITIVE,    &cfg.app_tag,           required_argument, app_tag},
		{"limited-retry",     'l', "",     CFG_NONE,        &cfg.limited_retry,     no_argument,       limited_retry},
		{"force-unit-access", 'f', "",     CFG_NONE,        &cfg.force_unit_access, no_argument,       force},
		{"show-command",      'v', "",     CFG_NONE,        &cfg.show,              no_argument,       show},
		{"dry-run",           'w', "",     CFG_NONE,        &cfg.dry_run,           no_argument,       dry},
		{"latency",           't', "",     CFG_NONE,        &cfg.latency,           no_argument,       latency},
		{0}
	};

	int fd = parse_and_open(argc, argv, desc, command_line_options, &cfg, sizeof(cfg));

	dfd = mfd = opcode & 1 ? STDIN_FILENO : STDOUT_FILENO;
	if (cfg.prinfo > 0xf)
		return EINVAL;
	control |= (cfg.prinfo << 10);
	if (cfg.limited_retry)
		control |= NVME_RW_LR;
	if (cfg.force_unit_access)
		control |= NVME_RW_FUA;
	if (strlen(cfg.data)){
		dfd = open(cfg.data, flags, mode);
		if (dfd < 0) {
			perror(cfg.data);
			return EINVAL;
		}
		mfd = dfd;
	}
	if (strlen(cfg.metadata)){
		mfd = open(cfg.metadata, flags, mode);
		if (mfd < 0) {
			perror(cfg.data);
			return EINVAL;
		}
	}

	if (!cfg.data_size)	{
		fprintf(stderr, "data size not provided\n");
		return EINVAL;
	}

	if (ioctl(fd, BLKPBSZGET, &phys_sector_size) < 0)
		return errno;

	buffer_size = ceiling(cfg.data_size, phys_sector_size);
	if (cfg.data_size < buffer_size) {
		fprintf(stderr, "Rounding data size to fit block count (%lld bytes)\n", buffer_size);
	}

	if (posix_memalign(&buffer, getpagesize(), buffer_size))
		return ENOMEM;
	memset(buffer, 0, cfg.data_size);

	if (cfg.metadata_size) {
		mbuffer = malloc(cfg.metadata_size);
		if (!mbuffer) {
 			free(buffer);
			return ENOMEM;
		}
	}

	if ((opcode & 1) && read(dfd, (void *)buffer, cfg.data_size) < 0) {
		fprintf(stderr, "failed to read data buffer from input file\n");
		err = EINVAL;
		goto free_and_return;
	}

	if ((opcode & 1) && cfg.metadata_size &&
				read(mfd, (void *)mbuffer, cfg.metadata_size) < 0) {
		fprintf(stderr, "failed to read meta-data buffer from input file\n");
		err = EINVAL;
		goto free_and_return;
	}

	if (cfg.show) {
		printf("opcode       : %02x\n", opcode);
		printf("flags        : %02x\n", 0);
		printf("control      : %04x\n", control);
		printf("rsvd         : %04x\n", 0);
		printf("metadata     : %"PRIx64"\n", (uint64_t)(uintptr_t)mbuffer);
		printf("addr         : %"PRIx64"\n", (uint64_t)(uintptr_t)buffer);
		printf("object_id    : %"PRIx64"\n", (uint64_t)cfg.object_id);
		printf("dsmgmt       : %08x\n", 0);
		printf("reftag       : %08x\n", cfg.ref_tag);
		printf("apptag       : %04x\n", cfg.app_tag);
		printf("appmask      : %04x\n", cfg.app_tag_mask);
		if (cfg.dry_run)
			goto free_and_return;
	}

	gettimeofday(&start_time, NULL);

	struct nvme_object_io io = {
		.opcode 	= opcode,
		.flags 		= 0,
		.control 	= control,
		.size		= buffer_size / phys_sector_size,
		.rsvd		= 0,
		.metadata	= (__u64)(uintptr_t) mbuffer,
		.addr		= (__u64)(uintptr_t) buffer,
		.object_id	= cfg.object_id,
		.dsmgmt		= 0,
		.reftag		= cfg.ref_tag,
		.appmask	= cfg.app_tag_mask,
		.apptag		= cfg.app_tag,
	};

	err = ioctl(fd, NVME_IOCTL_OBJECT_SUBMIT_IO, &io);

	gettimeofday(&end_time, NULL);
	if (cfg.latency)
		printf(" latency: %s: %llu us\n", command, elapsed_utime(start_time, end_time));
	if (err < 0)
		perror("submit-io");
	else if (err)
		printf("%s:%s(%04x)\n", command, nvme_status_to_string(err), err);
	else {
		if (!(opcode & 1) && write(dfd, (void *)buffer, cfg.data_size) < 0) {
			fprintf(stderr, "failed to write buffer to output file\n");
			err = EINVAL;
			goto free_and_return;
		} else if (!(opcode & 1) && cfg.metadata_size &&
				write(mfd, (void *)mbuffer, cfg.metadata_size) < 0) {
			fprintf(stderr, "failed to write meta-data buffer to output file\n");
			err = EINVAL;
			goto free_and_return;
		} else
			fprintf(stderr, "%s: Success\n", command);
	}
 free_and_return:
	free(buffer);
	if (cfg.metadata_size)
		free(mbuffer);
	return err;
}

static int write_object(int argc, char **argv, struct command *cmd, struct plugin *plugin)
{
	const char *desc = "Copy from provided data buffer (default "\
			"buffer is stdin) to specified object id on the given "\
			"device.";
	return submit_object_io(nvme_cmd_obj_write, "write", desc, argc, argv);
}

static int read_object(int argc, char **argv, struct command *cmd, struct plugin *plugin)
{
	return 0;
}
