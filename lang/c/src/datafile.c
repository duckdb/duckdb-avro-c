/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0 
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License. 
 */

#include "avro_private.h"
#include "avro/allocation.h"
#include "avro/generic.h"
#include "avro/errors.h"
#include "avro/value.h"
#include "encoding.h"
#include "codec.h"
#include "jansson.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <time.h>
#include <string.h>

struct avro_file_block_descriptor_t {
	int64_t data_offset;
	int64_t data_size;
	int64_t object_count;
};

struct avro_file_reader_t_ {
	avro_schema_t writers_schema;
	avro_reader_t reader;
	avro_reader_t block_reader;
	avro_codec_t codec;
	char sync[16];
	int64_t blocks_read;
	int64_t blocks_total;
	int64_t current_blocklen;
	char * current_blockdata;
	avro_value_iface_t *meta_iface;
	avro_value_t meta;
	const char *memory_buffer;
	int64_t memory_buffer_size;
	struct avro_file_block_descriptor_t *blocks;
	size_t block_count;
	size_t block_capacity;
};

struct avro_file_block_reader_t_ {
	avro_file_reader_t file_reader;
	avro_reader_t reader;
	avro_codec_t codec;
	int64_t objects_read;
	int64_t objects_total;
	int has_block;
};

struct avro_file_writer_t_ {
	avro_schema_t writers_schema;
	avro_writer_t writer;
	avro_codec_t codec;
	char sync[16];
	int block_count;
	size_t block_size;
	avro_writer_t datum_writer;
	char* datum_buffer;
	size_t datum_buffer_size;
	char schema_buf[64 * 1024];
};

#define DEFAULT_BLOCK_SIZE 16 * 1024

/* Note: We should not just read /dev/random here, because it may not
 * exist on all platforms e.g. Win32.
 */
static void generate_sync(avro_file_writer_t w)
{
	unsigned int i;
	srand(time(NULL));
	for (i = 0; i < sizeof(w->sync); i++) {
		w->sync[i] = ((double)rand() / (RAND_MAX + 1.0)) * 255;
	}
}

static int write_sync(avro_file_writer_t w)
{
	return avro_write(w->writer, w->sync, sizeof(w->sync));
}

static int write_header(avro_file_writer_t w, const char *metadata_json)
{
	int rval;
	uint8_t version = 1;
	/* TODO: remove this static buffer */
	avro_writer_t schema_writer;
	const avro_encoding_t *enc = &avro_binary_encoding;
	int64_t schema_len;

	/* Generate random sync */
	generate_sync(w);

	check(rval, avro_write(w->writer, "Obj", 3));
	check(rval, avro_write(w->writer, &version, 1));

	json_t *metadata_obj = NULL;
	json_error_t error;
	size_t meta_count = 0;

	if (metadata_json) {
		metadata_obj = json_loads(metadata_json, 0, &error);
		if (!metadata_obj) {
			// handle error: invalid JSON or not an object
			return EINVAL;
		}
		if (!json_is_object(metadata_obj)) {
			json_decref(metadata_obj);
			return EINVAL;
		}
		meta_count = json_object_size(metadata_obj);
	}

	check(rval, enc->write_long(w->writer, meta_count + 2)); // +2 for codec & schema

	check(rval, enc->write_string(w->writer, "avro.codec"));
	check(rval, enc->write_bytes(w->writer, w->codec->name, strlen(w->codec->name)));
	check(rval, enc->write_string(w->writer, "avro.schema"));
	schema_writer =
	    avro_writer_memory(&w->schema_buf[0], sizeof(w->schema_buf));
	rval = avro_schema_to_json(w->writers_schema, schema_writer);
	if (rval) {
		avro_writer_free(schema_writer);
		return rval;
	}
	schema_len = avro_writer_tell(schema_writer);
	avro_writer_free(schema_writer);
	rval = enc->write_bytes(w->writer, w->schema_buf, schema_len);
	if (rval) {
		json_decref(metadata_obj);
		return rval;
	}

	if (metadata_obj) {
		const char *key;
		json_t *value;

		json_object_foreach(metadata_obj, key, value) {
			rval = enc->write_string(w->writer, key);
			if (rval) {
				json_decref(metadata_obj);
				return rval;
			}
			if (!json_is_string(value)) {
				json_decref(metadata_obj);
				return EINVAL;
			}

			const char *val_str = json_string_value(value);
			rval = enc->write_bytes(w->writer, val_str, strlen(val_str));
			if (rval) {
				json_decref(metadata_obj);
				return rval;
			}
		}
		json_decref(metadata_obj);
	}

	check(rval, enc->write_long(w->writer, 0));
	return write_sync(w);
}

static int
file_writer_init_fp(FILE *fp, const char *path, int should_close, const char *mode, avro_file_writer_t w)
{
	if (!fp) {
		fp = fopen(path, mode);
	}

	if (!fp) {
		avro_set_error("Cannot open file for %s", path);
		return ENOMEM;
	}
	w->writer = avro_writer_file_fp(fp, should_close);
	if (!w->writer) {
		if (should_close) {
			fclose(fp);
		}
		avro_set_error("Cannot create file writer for %s", path);
		return ENOMEM;
	}
	return 0;
}

/* Exclusive file writing is supported by GCC using the mode
 * "wx". Win32 does not support exclusive file writing, so for win32
 * fall back to the non-exclusive file writing.
 */
#ifdef _WIN32
  #define EXCLUSIVE_WRITE_MODE   "wb"
#else
  #define EXCLUSIVE_WRITE_MODE   "wbx"
#endif

static int
file_writer_create(FILE *fp, const char *path, int should_close, avro_schema_t schema, avro_file_writer_t w, size_t block_size)
{
	int rval;

	w->block_count = 0;
	rval = file_writer_init_fp(fp, path, should_close, EXCLUSIVE_WRITE_MODE, w);
	if (rval) {
		check(rval, file_writer_init_fp(fp, path, should_close, "wb", w));
	}

	w->datum_buffer_size = block_size;
	w->datum_buffer = (char *) avro_malloc(w->datum_buffer_size);

	if(!w->datum_buffer) {
		avro_set_error("Could not allocate datum buffer\n");
		avro_writer_free(w->writer);
		return ENOMEM;
	}

	w->datum_writer =
	    avro_writer_memory(w->datum_buffer, w->datum_buffer_size);
	if (!w->datum_writer) {
		avro_set_error("Cannot create datum writer for file %s", path);
		avro_writer_free(w->writer);
		avro_free(w->datum_buffer, w->datum_buffer_size);
		return ENOMEM;
	}

	w->writers_schema = avro_schema_incref(schema);
	return write_header(w, NULL);
}

int
avro_file_writer_create(const char *path, avro_schema_t schema,
			avro_file_writer_t * writer)
{
	return avro_file_writer_create_with_codec_fp(NULL, path, 1, schema, writer, "null", 0);
}

int
avro_file_writer_create_fp(FILE *fp, const char *path, int should_close, avro_schema_t schema,
			avro_file_writer_t * writer)
{
	return avro_file_writer_create_with_codec_fp(fp, path, should_close, schema, writer, "null", 0);
}

int avro_file_writer_create_with_codec(const char *path,
			avro_schema_t schema, avro_file_writer_t * writer,
			const char *codec, size_t block_size)
{
	return avro_file_writer_create_with_codec_fp(NULL, path, 1, schema, writer, codec, block_size);
}

int avro_file_writer_create_with_codec_fp(FILE *fp, const char *path, int should_close,
			avro_schema_t schema, avro_file_writer_t * writer,
			const char *codec, size_t block_size)
{
	avro_file_writer_t w;
	int rval;
	check_param(EINVAL, path, "path");
	check_param(EINVAL, is_avro_schema(schema), "schema");
	check_param(EINVAL, writer, "writer");
	check_param(EINVAL, codec, "codec");

	if (block_size == 0) {
		block_size = DEFAULT_BLOCK_SIZE;
	}

	w = (avro_file_writer_t) avro_new(struct avro_file_writer_t_);
	if (!w) {
		avro_set_error("Cannot allocate new file writer");
		return ENOMEM;
	}
	w->codec = (avro_codec_t) avro_new(struct avro_codec_t_);
	if (!w->codec) {
		avro_set_error("Cannot allocate new codec");
		avro_freet(struct avro_file_writer_t_, w);
		return ENOMEM;
	}
	rval = avro_codec(w->codec, codec);
	if (rval) {
		avro_codec_reset(w->codec);
		avro_freet(struct avro_codec_t_, w->codec);
		avro_freet(struct avro_file_writer_t_, w);
		return rval;
	}
	rval = file_writer_create(fp, path, should_close, schema, w, block_size);
	if (rval) {
		avro_codec_reset(w->codec);
		avro_freet(struct avro_codec_t_, w->codec);
		avro_freet(struct avro_file_writer_t_, w);
		return rval;
	}
	*writer = w;

	return 0;
}

int avro_file_writer_create_from_writers_with_metadata_and_codec(avro_writer_t writer_in, avro_writer_t datum_writer_in, avro_schema_t schema, avro_file_writer_t * writer, const char *metadata_json, const char *codec) {
	avro_file_writer_t w;
	int rval;
	check_param(EINVAL, is_avro_schema(schema), "schema");
	check_param(EINVAL, writer, "writer");

	w = (avro_file_writer_t) avro_new(struct avro_file_writer_t_);
	if (!w) {
		avro_set_error("Cannot allocate new file writer");
		return ENOMEM;
	}
	w->block_count = 0;
	w->codec = (avro_codec_t) avro_new(struct avro_codec_t_);
	if (!w->codec) {
		avro_set_error("Cannot allocate new codec");
		avro_freet(struct avro_file_writer_t_, w);
		return ENOMEM;
	}
	/* A NULL codec selects the default ("null"/uncompressed); any avro-c codec
	 * name (e.g. "deflate", "snappy", "zstandard") is honoured. The block-write
	 * path (file_write_block) and header writer use w->codec uniformly, so the
	 * memory-writer flow compresses exactly like the path-based codec writer. */
	rval = avro_codec(w->codec, codec);
	if (rval) {
		avro_codec_reset(w->codec);
		avro_freet(struct avro_codec_t_, w->codec);
		avro_freet(struct avro_file_writer_t_, w);
		return rval;
	}
	w->writer = writer_in;
	*writer = w;

	w->datum_buffer_size = 0;
	w->datum_buffer = NULL;
	w->datum_writer = datum_writer_in;

	w->writers_schema = avro_schema_incref(schema);
	return write_header(w, metadata_json);
}

int avro_file_writer_create_from_writers_with_metadata(avro_writer_t writer_in, avro_writer_t datum_writer_in, avro_schema_t schema, avro_file_writer_t * writer, const char *metadata_json) {
	return avro_file_writer_create_from_writers_with_metadata_and_codec(writer_in, datum_writer_in, schema, writer, metadata_json, NULL);
}

int avro_file_writer_create_from_writers(avro_writer_t writer_in, avro_writer_t datum_writer_in, avro_schema_t schema, avro_file_writer_t * writer)
{
	return avro_file_writer_create_from_writers_with_metadata(writer_in, datum_writer_in, schema, writer, NULL);
}

static int file_read_header(avro_reader_t reader,
			    avro_schema_t * writers_schema, avro_codec_t codec,
			    char *sync, int synclen,
			    avro_value_iface_t **meta_iface_out, avro_value_t *meta_out)
{
	int rval;
	avro_schema_t meta_schema;
	avro_schema_t meta_values_schema;
	avro_value_iface_t *meta_iface;
	avro_value_t meta;
	char magic[4];
	avro_value_t codec_val;
	avro_value_t schema_bytes;
	const void *p;
	size_t len;

	check(rval, avro_read(reader, magic, sizeof(magic)));
	if (magic[0] != 'O' || magic[1] != 'b' || magic[2] != 'j'
	    || magic[3] != 1) {
		avro_set_error("Incorrect Avro container file magic number");
		return EILSEQ;
	}

	meta_values_schema = avro_schema_bytes();
	meta_schema = avro_schema_map(meta_values_schema, INT32_MAX, INT32_MAX);
	meta_iface = avro_generic_class_from_schema(meta_schema);
	if (meta_iface == NULL) {
		return EILSEQ;
	}
	check(rval, avro_generic_value_new(meta_iface, &meta));
	rval = avro_value_read(reader, &meta);
	if (rval) {
		avro_prefix_error("Cannot read file header: ");
		return EILSEQ;
	}
	avro_schema_decref(meta_schema);

	rval = avro_value_get_by_name(&meta, "avro.codec", &codec_val, NULL);
	if (rval) {
		if (avro_codec(codec, NULL) != 0) {
			avro_set_error("Codec not specified in header and unable to set 'null' codec");
			avro_value_decref(&meta);
			return EILSEQ;
		}
	} else {
		const void *buf;
		size_t size;
		char codec_name[11];

		avro_type_t type = avro_value_get_type(&codec_val);

		if (type != AVRO_BYTES) {
			avro_set_error("Value type of codec is unexpected");
			avro_value_decref(&meta);
			return EILSEQ;
		}

		avro_value_get_bytes(&codec_val, &buf, &size);
		memset(codec_name, 0, sizeof(codec_name));
		strncpy(codec_name, (const char *) buf, size < 10 ? size : 10);

		if (avro_codec(codec, codec_name) != 0) {
			avro_set_error("File header contains an unknown codec");
			avro_value_decref(&meta);
			return EILSEQ;
		}
	}

	rval = avro_value_get_by_name(&meta, "avro.schema", &schema_bytes, NULL);
	if (rval) {
		avro_set_error("File header doesn't contain a schema");
		avro_value_decref(&meta);
		return EILSEQ;
	}

	avro_value_get_bytes(&schema_bytes, &p, &len);
	rval = avro_schema_from_json_length((const char *) p, len, writers_schema);
	if (rval) {
		avro_prefix_error("Cannot parse file header: ");
		avro_value_decref(&meta);
		return rval;
	}

	if (meta_iface_out && meta_out) {
		*meta_iface_out = meta_iface;
		*meta_out = meta;
	} else {
		avro_value_decref(&meta);
		avro_value_iface_decref(meta_iface);
	}
	return avro_read(reader, sync, synclen);
}

static int
file_writer_open(const char *path, avro_file_writer_t w, size_t block_size)
{
	int rval;
	FILE *fp;
	avro_reader_t reader;

	/* Open for read AND write */
	fp = fopen(path, "r+b");
	if (!fp) {
		avro_set_error("Error opening file: %s",
			       strerror(errno));
		return errno;
	}

	/* Don`t close the underlying file descriptor, logrotate can
	 * vanish it from sight. */
	reader = avro_reader_file_fp(fp, 0);
	if (!reader) {
		fclose(fp);
		avro_set_error("Cannot create file reader for %s", path);
		return ENOMEM;
	}
	rval =
	    file_read_header(reader, &w->writers_schema, w->codec, w->sync,
			     sizeof(w->sync), NULL, NULL);

	avro_reader_free(reader);
	if (rval) {
		fclose(fp);
		return rval;
	}

	w->block_count = 0;

	/* Position to end of file and get ready to write */
	fseek(fp, 0, SEEK_END);

	w->writer = avro_writer_file(fp);
	if (!w->writer) {
		fclose(fp);
		avro_set_error("Cannot create file writer for %s", path);
		return ENOMEM;
	}

	if (block_size == 0) {
		block_size = DEFAULT_BLOCK_SIZE;
	}

	w->datum_buffer_size = block_size;
	w->datum_buffer = (char *) avro_malloc(w->datum_buffer_size);

	if(!w->datum_buffer) {
		avro_set_error("Could not allocate datum buffer\n");
		avro_writer_free(w->writer);
		return ENOMEM;
	}

	w->datum_writer =
	    avro_writer_memory(w->datum_buffer, w->datum_buffer_size);
	if (!w->datum_writer) {
		avro_set_error("Cannot create datum writer for file %s", path);
		avro_writer_free(w->writer);
		avro_free(w->datum_buffer, w->datum_buffer_size);
		return ENOMEM;
	}

	return 0;
}

int
avro_file_writer_open_bs(const char *path, avro_file_writer_t * writer,
			 size_t block_size)
{
	avro_file_writer_t w;
	int rval;
	check_param(EINVAL, path, "path");
	check_param(EINVAL, writer, "writer");

	w = (avro_file_writer_t) avro_new(struct avro_file_writer_t_);
	if (!w) {
		avro_set_error("Cannot create new file writer for %s", path);
		return ENOMEM;
	}
	w->codec = (avro_codec_t) avro_new(struct avro_codec_t_);
	if (!w->codec) {
		avro_set_error("Cannot allocate new codec");
		avro_freet(struct avro_file_writer_t_, w);
		return ENOMEM;
	}
	avro_codec(w->codec, NULL);
	rval = file_writer_open(path, w, block_size);
	if (rval) {
		avro_codec_reset(w->codec);
		avro_freet(struct avro_codec_t_, w->codec);
		avro_freet(struct avro_file_writer_t_, w);
		return rval;
	}

	*writer = w;
	return 0;
}

int
avro_file_writer_open(const char *path, avro_file_writer_t * writer)
{
	return avro_file_writer_open_bs(path, writer, 0);
}

static int file_add_block_descriptor(avro_file_reader_t r,
			int64_t data_offset, int64_t data_size, int64_t object_count)
{
	struct avro_file_block_descriptor_t *new_blocks;
	size_t old_size;
	size_t new_size;
	size_t new_capacity;

	if (r->block_count == r->block_capacity) {
		new_capacity = r->block_capacity == 0 ? 8 : r->block_capacity * 2;
		if (new_capacity < r->block_capacity ||
		    new_capacity > SIZE_MAX / sizeof(struct avro_file_block_descriptor_t)) {
			avro_set_error("Too many Avro file blocks");
			return ERANGE;
		}
		old_size = r->block_capacity * sizeof(struct avro_file_block_descriptor_t);
		new_size = new_capacity * sizeof(struct avro_file_block_descriptor_t);
		new_blocks = (struct avro_file_block_descriptor_t *)
			avro_realloc(r->blocks, old_size, new_size);
		if (!new_blocks) {
			avro_set_error("Cannot allocate Avro file block index");
			return ENOMEM;
		}
		r->blocks = new_blocks;
		r->block_capacity = new_capacity;
	}

	r->blocks[r->block_count].data_offset = data_offset;
	r->blocks[r->block_count].data_size = data_size;
	r->blocks[r->block_count].object_count = object_count;
	r->block_count++;
	return 0;
}

static int file_index_memory_blocks(avro_file_reader_t r, int64_t data_offset)
{
	const avro_encoding_t *enc = &avro_binary_encoding;
	avro_reader_t index_reader;
	char sync[16];
	int64_t object_count;
	int64_t block_size;
	int64_t relative_offset;
	int64_t block_offset;
	int rval;

	index_reader = avro_reader_memory(r->memory_buffer + data_offset,
			r->memory_buffer_size - data_offset);
	if (!index_reader) {
		return ENOMEM;
	}

	while (!avro_reader_memory_is_depleted(index_reader)) {
		rval = enc->read_long(index_reader, &object_count);
		if (rval) {
			avro_prefix_error("Cannot index file block count: ");
			avro_reader_free(index_reader);
			return rval;
		}
		rval = enc->read_long(index_reader, &block_size);
		if (rval) {
			avro_prefix_error("Cannot index file block size: ");
			avro_reader_free(index_reader);
			return rval;
		}
		if (object_count <= 0 || block_size < 0) {
			avro_set_error("Invalid Avro file block count or size");
			avro_reader_free(index_reader);
			return EILSEQ;
		}

		relative_offset = avro_reader_tell(index_reader);
		if (relative_offset < 0 || data_offset > INT64_MAX - relative_offset) {
			avro_set_error("Avro file block offset overflow");
			avro_reader_free(index_reader);
			return ERANGE;
		}
		block_offset = data_offset + relative_offset;
		if (block_offset > r->memory_buffer_size ||
		    block_size > r->memory_buffer_size - block_offset) {
			avro_set_error("Avro file block exceeds memory buffer");
			avro_reader_free(index_reader);
			return EILSEQ;
		}

		rval = avro_skip(index_reader, block_size);
		if (rval) {
			avro_prefix_error("Cannot index file block: ");
			avro_reader_free(index_reader);
			return rval;
		}
		rval = avro_read(index_reader, sync, sizeof(sync));
		if (rval) {
			avro_prefix_error("Cannot index file block sync: ");
			avro_reader_free(index_reader);
			return rval;
		}
		if (memcmp(r->sync, sync, sizeof(r->sync)) != 0) {
			avro_set_error("Incorrect sync bytes");
			avro_reader_free(index_reader);
			return EILSEQ;
		}
		rval = file_add_block_descriptor(r, block_offset, block_size, object_count);
		if (rval) {
			avro_reader_free(index_reader);
			return rval;
		}
	}

	avro_reader_free(index_reader);
	return 0;
}

static int file_read_block_count(avro_file_reader_t r)
{
	int rval;
	int64_t len;
	const avro_encoding_t *enc = &avro_binary_encoding;

	/* For a correctly formatted file, EOF will occur here */
	rval = enc->read_long(r->reader, &r->blocks_total);
	if (rval == EOF) {
		return rval;
	}

	if (rval == EILSEQ && avro_reader_is_eof(r->reader)) {
		return EOF;
	}

	check_prefix(rval, rval,
		     "Cannot read file block count: ");
	check_prefix(rval, enc->read_long(r->reader, &len),
		     "Cannot read file block size: ");
	if (r->blocks_total <= 0 || len < 0) {
		avro_set_error("Invalid Avro file block count or size");
		return EILSEQ;
	}

	if (r->current_blockdata && len > r->current_blocklen) {
		r->current_blockdata = (char *) avro_realloc(r->current_blockdata, r->current_blocklen, len);
		r->current_blocklen = len;
	} else if (!r->current_blockdata) {
		r->current_blockdata = (char *) avro_malloc(len);
		r->current_blocklen = len;
	}

	if (len > 0) {
		check_prefix(rval, avro_read(r->reader, r->current_blockdata, len),
			     "Cannot read file block: ");

		check_prefix(rval, avro_codec_decode(r->codec, r->current_blockdata, len),
			     "Cannot decode file block: ");
	}

	avro_reader_memory_set_source(r->block_reader, (const char *) r->codec->block_data, r->codec->used_size);

	r->blocks_read = 0;
	return 0;
}

int avro_file_reader_fp(FILE *fp, const char *path, int should_close,
			avro_file_reader_t * reader)
{
	int rval;
	avro_file_reader_t r = (avro_file_reader_t) avro_new(struct avro_file_reader_t_);
	if (!r) {
		if (should_close) {
			fclose(fp);
		}
		avro_set_error("Cannot allocate file reader for %s", path);
		return ENOMEM;
	}
	memset(r, 0, sizeof(struct avro_file_reader_t_));

	r->reader = avro_reader_file_fp(fp, should_close);
	if (!r->reader) {
		if (should_close) {
			fclose(fp);
		}
		avro_set_error("Cannot allocate reader for file %s", path);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}
	r->block_reader = avro_reader_memory(0, 0);
	if (!r->block_reader) {
		avro_set_error("Cannot allocate block reader for file %s", path);
		avro_reader_free(r->reader);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}

	r->codec = (avro_codec_t) avro_new(struct avro_codec_t_);
	if (!r->codec) {
		avro_set_error("Could not allocate codec for file %s", path);
		avro_reader_free(r->reader);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}
	avro_codec(r->codec, NULL);

	rval = file_read_header(r->reader, &r->writers_schema, r->codec,
				r->sync, sizeof(r->sync), &r->meta_iface, &r->meta);
	if (rval) {
		avro_reader_free(r->reader);
		avro_codec_reset(r->codec);
		avro_freet(struct avro_codec_t_, r->codec);
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	r->current_blockdata = NULL;
	r->current_blocklen = 0;

	rval = file_read_block_count(r);
	if (rval == EOF) {
		r->blocks_total = 0;
	} else if (rval) {
		avro_reader_free(r->reader);
		avro_codec_reset(r->codec);
		avro_freet(struct avro_codec_t_, r->codec);
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	*reader = r;
	return 0;
}

int avro_reader_reader(avro_reader_t reader_in,	avro_file_reader_t * reader)
{
	if (!avro_reader_is_memory(reader_in)) {
		avro_set_error("Cannot create a file_reader from a non-memory reader");
		return EINVAL;
	}

	const char* path = "";
	int rval;
	avro_file_reader_t r = (avro_file_reader_t) avro_new(struct avro_file_reader_t_);
	if (!r) {
		avro_set_error("Cannot allocate file reader for %s", path);
		return ENOMEM;
	}
	memset(r, 0, sizeof(struct avro_file_reader_t_));

	r->reader = reader_in;
	if (!r->reader) {
		avro_set_error("Cannot allocate reader for file %s", path);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}
	r->block_reader = avro_reader_memory(0, 0);
	if (!r->block_reader) {
		avro_set_error("Cannot allocate block reader for file %s", path);
		avro_reader_free(r->reader);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}

	r->codec = (avro_codec_t) avro_new(struct avro_codec_t_);
	if (!r->codec) {
		avro_set_error("Could not allocate codec for file %s", path);
		avro_reader_free(r->reader);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}
	avro_codec(r->codec, NULL);

	rval = file_read_header(r->reader, &r->writers_schema, r->codec,
				r->sync, sizeof(r->sync), &r->meta_iface, &r->meta);
	if (rval) {
		avro_reader_free(r->reader);
		avro_codec_reset(r->codec);
		avro_freet(struct avro_codec_t_, r->codec);
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	r->current_blockdata = NULL;
	r->current_blocklen = 0;

	if (avro_reader_memory_is_depleted(r->reader)) {
		rval = EOF;
	} else {
		rval = file_read_block_count(r);
	}

	if (rval == EOF) {
		r->blocks_total = 0;
	} else if (rval) {
		avro_reader_free(r->reader);
		avro_codec_reset(r->codec);
		avro_freet(struct avro_codec_t_, r->codec);
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	*reader = r;
	return 0;
}

int avro_file_reader_memory(const char *buf, int64_t len,
			avro_file_reader_t *reader)
{
	avro_file_reader_t r;
	int64_t data_offset;
	int rval;

	check_param(EINVAL, buf || len == 0, "buffer");
	check_param(EINVAL, len >= 0, "buffer length");
	check_param(EINVAL, reader, "reader");

	r = (avro_file_reader_t) avro_new(struct avro_file_reader_t_);
	if (!r) {
		avro_set_error("Cannot allocate memory file reader");
		return ENOMEM;
	}
	memset(r, 0, sizeof(struct avro_file_reader_t_));
	r->memory_buffer = buf;
	r->memory_buffer_size = len;
	r->reader = avro_reader_memory(buf, len);
	if (!r->reader) {
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}
	r->block_reader = avro_reader_memory(0, 0);
	if (!r->block_reader) {
		avro_reader_free(r->reader);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}
	r->codec = (avro_codec_t) avro_new(struct avro_codec_t_);
	if (!r->codec) {
		avro_reader_free(r->reader);
		avro_reader_free(r->block_reader);
		avro_freet(struct avro_file_reader_t_, r);
		return ENOMEM;
	}
	avro_codec(r->codec, NULL);

	rval = file_read_header(r->reader, &r->writers_schema, r->codec,
			r->sync, sizeof(r->sync), &r->meta_iface, &r->meta);
	if (rval) {
		avro_reader_free(r->reader);
		avro_reader_free(r->block_reader);
		avro_codec_reset(r->codec);
		avro_freet(struct avro_codec_t_, r->codec);
		avro_freet(struct avro_file_reader_t_, r);
		return rval;
	}

	data_offset = avro_reader_tell(r->reader);
	if (data_offset < 0 || data_offset > len) {
		avro_set_error("Invalid Avro file header size");
		rval = EILSEQ;
		goto error_after_header;
	}
	rval = file_index_memory_blocks(r, data_offset);
	if (rval) {
		goto error_after_header;
	}

	if (avro_reader_memory_is_depleted(r->reader)) {
		r->blocks_total = 0;
	} else {
		rval = file_read_block_count(r);
		if (rval == EOF) {
			r->blocks_total = 0;
		} else if (rval) {
			goto error_after_header;
		}
	}

	*reader = r;
	return 0;

error_after_header:
	avro_schema_decref(r->writers_schema);
	avro_reader_free(r->reader);
	avro_reader_free(r->block_reader);
	avro_codec_reset(r->codec);
	avro_freet(struct avro_codec_t_, r->codec);
	if (r->current_blockdata) {
		avro_free(r->current_blockdata, r->current_blocklen);
	}
	if (r->blocks) {
		avro_free(r->blocks, r->block_capacity * sizeof(struct avro_file_block_descriptor_t));
	}
	avro_value_decref(&r->meta);
	avro_value_iface_decref(r->meta_iface);
	avro_freet(struct avro_file_reader_t_, r);
	return rval;
}

int avro_file_reader(const char *path, avro_file_reader_t * reader)
{
	FILE *fp;

	fp = fopen(path, "rb");
	if (!fp) {
		return errno;
	}

	return avro_file_reader_fp(fp, path, 1, reader);
}

avro_schema_t
avro_file_reader_get_writer_schema(avro_file_reader_t r)
{
	check_param(NULL, r, "reader");
	return avro_schema_incref(r->writers_schema);
}

const char* avro_file_reader_get_metadata(avro_file_reader_t reader, const char *key)
{
	if (!reader || !key) {
		return NULL;
	}

	avro_value_t meta_val;
	int rval;

	rval = avro_value_get_by_name(&reader->meta, key, &meta_val, NULL);
	if (rval) {
		return NULL;
	}

	if (avro_value_get_type(&meta_val) != AVRO_BYTES) {
		return NULL;
	}

	const void *buf;
	size_t size;

	if (avro_value_get_bytes(&meta_val, &buf, &size) != 0) {
		return NULL;
	}

	return (const char *)buf;
}

int avro_file_reader_get_metadata_count(avro_file_reader_t reader, size_t *count)
{
	if (!reader || !count) {
		return EINVAL;
	}

	return avro_value_get_size(&reader->meta, count);
}

int avro_file_reader_get_metadata_by_index(avro_file_reader_t reader, size_t index, const char **key, const char **value, size_t *value_size)
{
	if (!reader) {
		return EINVAL;
	}

	avro_value_t meta_entry;
	const char *entry_key;
	int rval;

	rval = avro_value_get_by_index(&reader->meta, index, &meta_entry, &entry_key);
	if (rval) {
		return rval;
	}

	if (key) {
		*key = entry_key;
	}

	if (value || value_size) {
		if (avro_value_get_type(&meta_entry) != AVRO_BYTES) {
			return EINVAL;
		}

		const void *buf;
		size_t size;

		rval = avro_value_get_bytes(&meta_entry, &buf, &size);
		if (rval) {
			return rval;
		}

		if (value) {
			*value = (const char *)buf;
		}
		if (value_size) {
			*value_size = size;
		}
	}
	return 0;
}

int avro_file_reader_get_block_count(avro_file_reader_t reader, size_t *count)
{
	check_param(EINVAL, reader, "reader");
	check_param(EINVAL, count, "count");
	if (!reader->memory_buffer) {
		avro_set_error("Block indexing is only available for memory file readers");
		return EINVAL;
	}
	*count = reader->block_count;
	return 0;
}

int avro_file_block_reader_create(avro_file_reader_t reader,
			avro_file_block_reader_t *block_reader)
{
	avro_file_block_reader_t result;
	int rval;

	check_param(EINVAL, reader, "reader");
	check_param(EINVAL, block_reader, "block reader");
	if (!reader->memory_buffer) {
		avro_set_error("Block readers require a memory file reader");
		return EINVAL;
	}

	result = (avro_file_block_reader_t) avro_new(struct avro_file_block_reader_t_);
	if (!result) {
		avro_set_error("Cannot allocate Avro file block reader");
		return ENOMEM;
	}
	memset(result, 0, sizeof(struct avro_file_block_reader_t_));
	result->file_reader = reader;
	result->reader = avro_reader_memory(0, 0);
	if (!result->reader) {
		avro_freet(struct avro_file_block_reader_t_, result);
		return ENOMEM;
	}
	result->codec = (avro_codec_t) avro_new(struct avro_codec_t_);
	if (!result->codec) {
		avro_reader_free(result->reader);
		avro_freet(struct avro_file_block_reader_t_, result);
		return ENOMEM;
	}
	rval = avro_codec(result->codec, reader->codec->name);
	if (rval) {
		avro_reader_free(result->reader);
		avro_freet(struct avro_codec_t_, result->codec);
		avro_freet(struct avro_file_block_reader_t_, result);
		return rval;
	}

	*block_reader = result;
	return 0;
}

int avro_file_block_reader_select_block(avro_file_block_reader_t block_reader,
			size_t block_index)
{
	struct avro_file_block_descriptor_t *block;
	const char *block_data;
	int rval;

	check_param(EINVAL, block_reader, "block reader");
	if (block_index >= block_reader->file_reader->block_count) {
		avro_set_error("Avro file block index out of range");
		return EINVAL;
	}

	block = &block_reader->file_reader->blocks[block_index];
	block_data = block_reader->file_reader->memory_buffer + block->data_offset;
	rval = avro_codec_decode(block_reader->codec, (void *) block_data, block->data_size);
	if (rval) {
		avro_prefix_error("Cannot decode Avro file block: ");
		return rval;
	}
	avro_reader_memory_set_source(block_reader->reader,
			(const char *) block_reader->codec->block_data,
			block_reader->codec->used_size);
	block_reader->objects_read = 0;
	block_reader->objects_total = block->object_count;
	block_reader->has_block = 1;
	return 0;
}

int avro_file_block_reader_read_value(avro_file_block_reader_t block_reader,
			avro_value_t *dest)
{
	int rval;

	check_param(EINVAL, block_reader, "block reader");
	check_param(EINVAL, dest, "value");
	if (!block_reader->has_block) {
		avro_set_error("No Avro file block selected");
		return EINVAL;
	}
	if (block_reader->objects_read == block_reader->objects_total) {
		if (!avro_reader_is_eof(block_reader->reader)) {
			avro_set_error("Avro file block contains trailing data");
			return EILSEQ;
		}
		return EOF;
	}

	rval = avro_value_read(block_reader->reader, dest);
	if (rval) {
		if (rval == EOF) {
			avro_set_error("Avro file block ended before its declared object count");
			return EILSEQ;
		}
		return rval;
	}
	block_reader->objects_read++;
	return 0;
}

int avro_file_block_reader_close(avro_file_block_reader_t block_reader)
{
	check_param(EINVAL, block_reader, "block reader");
	avro_reader_free(block_reader->reader);
	avro_codec_reset(block_reader->codec);
	avro_freet(struct avro_codec_t_, block_reader->codec);
	avro_freet(struct avro_file_block_reader_t_, block_reader);
	return 0;
}

static int file_write_block(avro_file_writer_t w)
{
	const avro_encoding_t *enc = &avro_binary_encoding;
	int rval;

	if (w->block_count) {
		/* Write the block count */
		check_prefix(rval, enc->write_long(w->writer, w->block_count),
			     "Cannot write file block count: ");
		/* Encode the block */
		check_prefix(rval, avro_codec_encode(w->codec, (void *)avro_writer_buf(w->datum_writer), w->block_size),
			     "Cannot encode file block: ");
		/* Write the block length */
		check_prefix(rval, enc->write_long(w->writer, w->codec->used_size),
			     "Cannot write file block size: ");
		/* Write the block */
		check_prefix(rval, avro_write(w->writer, w->codec->block_data, w->codec->used_size),
			     "Cannot write file block: ");
		/* Write the sync marker */
		check_prefix(rval, write_sync(w),
			     "Cannot write sync marker: ");
		/* Reset the datum writer */
		avro_writer_reset(w->datum_writer);
		w->block_count = 0;
		w->block_size = 0;
	}
	return 0;
}

int avro_file_writer_append(avro_file_writer_t w, avro_datum_t datum)
{
	int rval;
	check_param(EINVAL, w, "writer");
	check_param(EINVAL, datum, "datum");

	rval = avro_write_data(w->datum_writer, w->writers_schema, datum);
	if (rval) {
		check(rval, file_write_block(w));
		rval =
		    avro_write_data(w->datum_writer, w->writers_schema, datum);
		if (rval) {
			avro_set_error("Datum too large for file block size");
			/* TODO: if the datum encoder larger than our buffer,
			   just write a single large datum */
			return rval;
		}
	}
	w->block_count++;
	w->block_size = avro_writer_tell(w->datum_writer);
	return 0;
}

int
avro_file_writer_append_value(avro_file_writer_t w, avro_value_t *value)
{
	int rval;
	check_param(EINVAL, w, "writer");
	check_param(EINVAL, value, "value");

	rval = avro_value_write(w->datum_writer, value);
	if (rval) {
		check(rval, file_write_block(w));
		rval = avro_value_write(w->datum_writer, value);
		if (rval) {
			avro_set_error("Value too large for file block size");
			/* TODO: if the value encoder larger than our buffer,
			   just write a single large datum */
			return rval;
		}
	}
	w->block_count++;
	w->block_size = avro_writer_tell(w->datum_writer);
	return 0;
}

int
avro_file_writer_append_encoded(avro_file_writer_t w,
				const void *buf, int64_t len)
{
	int rval;
	check_param(EINVAL, w, "writer");

	rval = avro_write(w->datum_writer, (void *) buf, len);
	if (rval) {
		check(rval, file_write_block(w));
		rval = avro_write(w->datum_writer, (void *) buf, len);
		if (rval) {
			avro_set_error("Value too large for file block size");
			/* TODO: if the value encoder larger than our buffer,
			   just write a single large datum */
			return rval;
		}
	}
	w->block_count++;
	w->block_size = avro_writer_tell(w->datum_writer);
	return 0;
}

int avro_file_writer_sync(avro_file_writer_t w)
{
	return file_write_block(w);
}

int avro_file_writer_flush(avro_file_writer_t w)
{
	int rval;
	check(rval, file_write_block(w));
	avro_writer_flush(w->writer);
	return 0;
}

int avro_file_writer_close(avro_file_writer_t w)
{
	int rval;
	check(rval, avro_file_writer_flush(w));
	avro_schema_decref(w->writers_schema);
	avro_writer_free(w->datum_writer);
	avro_writer_free(w->writer);
	avro_free(w->datum_buffer, w->datum_buffer_size);
	avro_codec_reset(w->codec);
	avro_freet(struct avro_codec_t_, w->codec);
	avro_freet(struct avro_file_writer_t_, w);
	return 0;
}

int avro_file_reader_read(avro_file_reader_t r, avro_schema_t readers_schema,
			  avro_datum_t * datum)
{
	int rval;
	char sync[16];

	check_param(EINVAL, r, "reader");
	check_param(EINVAL, datum, "datum");

	/* This will be set to zero when an empty file is opened.
	 * Return EOF here when the user attempts to read. */
	if (r->blocks_total == 0) {
		return EOF;
	}

	if (r->blocks_read == r->blocks_total) {
		check(rval, avro_read(r->reader, sync, sizeof(sync)));
		if (memcmp(r->sync, sync, sizeof(r->sync)) != 0) {
			/* wrong sync bytes */
			avro_set_error("Incorrect sync bytes");
			return EILSEQ;
		}
		check(rval, file_read_block_count(r));
	}

	check(rval,
	      avro_read_data(r->block_reader, r->writers_schema, readers_schema,
			     datum));
	r->blocks_read++;

	return 0;
}

int
avro_file_reader_read_value(avro_file_reader_t r, avro_value_t *value)
{
	int rval;
	char sync[16];

	check_param(EINVAL, r, "reader");
	check_param(EINVAL, value, "value");

	/* This will be set to zero when an empty file is opened.
	 * Return EOF here when the user attempts to read. */
	if (r->blocks_total == 0) {
		return EOF;
	}

	if (r->blocks_read == r->blocks_total) {
		/* reads sync bytes and buffers further bytes */
		check(rval, avro_read(r->reader, sync, sizeof(sync)));
		if (memcmp(r->sync, sync, sizeof(r->sync)) != 0) {
			/* wrong sync bytes */
			avro_set_error("Incorrect sync bytes");
			return EILSEQ;
		}

		check(rval, file_read_block_count(r));
	}

	check(rval, avro_value_read(r->block_reader, value));
	r->blocks_read++;

	return 0;
}

int avro_file_reader_close(avro_file_reader_t reader)
{
	avro_schema_decref(reader->writers_schema);
	avro_reader_free(reader->reader);
	avro_reader_free(reader->block_reader);
	avro_codec_reset(reader->codec);
	avro_freet(struct avro_codec_t_, reader->codec);
	if (reader->current_blockdata) {
		avro_free(reader->current_blockdata, reader->current_blocklen);
	}
	if (reader->blocks) {
		avro_free(reader->blocks,
			reader->block_capacity * sizeof(struct avro_file_block_descriptor_t));
	}
	avro_value_decref(&reader->meta);
	avro_value_iface_decref(reader->meta_iface);
	avro_freet(struct avro_file_reader_t_, reader);
	return 0;
}
