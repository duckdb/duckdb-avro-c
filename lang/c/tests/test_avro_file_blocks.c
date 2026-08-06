/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information.
 */

#include <avro.h>
#include <stdio.h>
#include <stdlib.h>

#define check(call) do { \
	int check_result__ = (call); \
	if (check_result__ != 0) { \
		fprintf(stderr, "%s failed: %s\n", #call, avro_strerror()); \
		exit(EXIT_FAILURE); \
	} \
} while (0)

#define check_true(condition) do { \
	if (!(condition)) { \
		fprintf(stderr, "%s failed\n", #condition); \
		exit(EXIT_FAILURE); \
	} \
} while (0)

static const char *filename = "avro-file-blocks.avro";
static const char schema_json[] =
	"{\"type\":\"record\",\"name\":\"row\","
	"\"fields\":[{\"name\":\"value\",\"type\":\"long\"}]}";

static void set_value(avro_value_t *value, int64_t input)
{
	avro_value_t field;
	check(avro_value_get_by_index(value, 0, &field, NULL));
	check(avro_value_set_long(&field, input));
}

static int64_t get_value(avro_value_t *value)
{
	avro_value_t field;
	int64_t result;
	check(avro_value_get_by_index(value, 0, &field, NULL));
	check(avro_value_get_long(&field, &result));
	return result;
}

int main(void)
{
	avro_schema_t schema;
	avro_schema_error_t schema_error;
	avro_value_iface_t *iface;
	avro_value_t value;
	avro_value_t first;
	avro_value_t second;
	avro_file_writer_t writer;
	avro_file_reader_t reader;
	avro_file_block_reader_t first_reader;
	avro_file_block_reader_t second_reader;
	FILE *file;
	char *buffer;
	char last_byte;
	long file_size;
	size_t block_count;
	int i;

	check(avro_schema_from_json(schema_json, 0, &schema, &schema_error));
	iface = avro_generic_class_from_schema(schema);
	check_true(iface != NULL);
	check(avro_generic_value_new(iface, &value));
	check(avro_file_writer_create(filename, schema, &writer));

	for (i = 0; i < 12; i++) {
		set_value(&value, i);
		check(avro_file_writer_append_value(writer, &value));
		avro_value_reset(&value);
		if (i % 3 == 2) {
			check(avro_file_writer_sync(writer));
		}
	}
	check(avro_file_writer_close(writer));
	avro_value_decref(&value);

	file = fopen(filename, "rb");
	check_true(file != NULL);
	check_true(fseek(file, 0, SEEK_END) == 0);
	file_size = ftell(file);
	check_true(file_size > 0);
	check_true(fseek(file, 0, SEEK_SET) == 0);
	buffer = (char *) malloc((size_t) file_size);
	check_true(buffer != NULL);
	check_true(fread(buffer, (size_t) file_size, 1, file) == 1);
	fclose(file);

	/* The block indexer rejects truncated files and invalid sync markers. */
	check_true(avro_file_reader_memory(buffer, file_size - 1, &reader) != 0);
	last_byte = buffer[file_size - 1];
	buffer[file_size - 1] ^= 1;
	check_true(avro_file_reader_memory(buffer, file_size, &reader) != 0);
	buffer[file_size - 1] = last_byte;

	check(avro_file_reader_memory(buffer, file_size, &reader));
	check(avro_file_reader_get_block_count(reader, &block_count));
	check_true(block_count == 4);
	check(avro_file_block_reader_create(reader, &first_reader));
	check(avro_file_block_reader_create(reader, &second_reader));
	check(avro_generic_value_new(iface, &first));
	check(avro_generic_value_new(iface, &second));

	check(avro_file_block_reader_select_block(first_reader, 3));
	check(avro_file_block_reader_select_block(second_reader, 0));
	for (i = 0; i < 3; i++) {
		check(avro_file_block_reader_read_value(first_reader, &first));
		check(avro_file_block_reader_read_value(second_reader, &second));
		check_true(get_value(&first) == i + 9);
		check_true(get_value(&second) == i);
		avro_value_reset(&first);
		avro_value_reset(&second);
	}
	check_true(avro_file_block_reader_read_value(first_reader, &first) == EOF);
	check_true(avro_file_block_reader_read_value(second_reader, &second) == EOF);

	check(avro_file_block_reader_select_block(second_reader, 2));
	for (i = 6; i < 9; i++) {
		check(avro_file_block_reader_read_value(second_reader, &second));
		check_true(get_value(&second) == i);
		avro_value_reset(&second);
	}
	check_true(avro_file_block_reader_read_value(second_reader, &second) == EOF);
	check_true(avro_file_block_reader_select_block(first_reader, block_count) == EINVAL);

	avro_value_decref(&first);
	avro_value_decref(&second);
	check(avro_file_block_reader_close(first_reader));
	check(avro_file_block_reader_close(second_reader));
	check(avro_file_reader_close(reader));
	avro_value_iface_decref(iface);
	avro_schema_decref(schema);
	free(buffer);
	remove(filename);
	return EXIT_SUCCESS;
}
