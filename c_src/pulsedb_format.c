#include <stdint.h>
#include "erl_nif.h"
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>

struct BitReader {
  unsigned char *bytes;
  unsigned size;
  unsigned offset;
};

static inline void bit_init(struct BitReader *br, unsigned char *bytes, unsigned size) {
  br->bytes = bytes;
  br->size = size;
  br->offset = 0;
}

static inline unsigned bits_remain(struct BitReader *br) {
  return br->size*8 - br->offset;
}

static inline int bits_skip(struct BitReader *br, unsigned size) {
  if(br->offset + size > br->size*8) return 0;
  br->offset += size;
  return 1;
}

static inline unsigned char *bits_head(struct BitReader *br) {
  return &br->bytes[br->offset / 8];
}

static inline int bit_look(struct BitReader *br, unsigned char *bit) {
  unsigned offset = br->offset;
  if(br->offset == br->size*8) return 0;
  *bit = (br->bytes[offset / 8] >> (7 - (offset % 8))) & 1;
  return 1;
}

static inline int bit_get(struct BitReader *br, unsigned char *bit) {
  if(!bit_look(br, bit)) return 0;
  br->offset++;
  // fprintf(stderr, "Bit: %d\r\n", bit);
  return 1;
}

static inline int bits_read32(struct BitReader *br, int32_t *result) {
  if(br->offset + 32 > br->size*8 || br->offset % 8 != 0) return 0;
  unsigned char *p1,*p2;
  p1 = &br->bytes[br->offset / 8];
  p2 = (unsigned char *)result;
  
  p2[0] = p1[3];
  p2[1] = p1[2];
  p2[2] = p1[1];
  p2[3] = p1[0];
  br->offset += 32;
  return 1;
}




static inline int bits_get(struct BitReader *br, int bits, uint64_t *result_p) {
  uint64_t result = 0;
  if(br->offset + bits > br->size*8) return 0;
  
  if(bits <= 8 && br->offset & 0x7) {
    while(bits) {
      unsigned char bit;
      if(!bit_get(br, &bit)) return 0;
      result = (result << 1) | bit;
      bits--;
    }
    *result_p = result;
    return 1;
  }
  
  if(bits == 32 && br->offset % 8 == 0) {
    result = ntohl(*(uint32_t *)(&br->bytes[br->offset / 8]));
    br->offset += 32;
    *result_p = result;
    return 1;
  }

  while(bits) {
    while(bits >= 8 && !(br->offset & 0x7)) {
      bits -= 8;
      result = (result << 8) | br->bytes[br->offset >> 3];
      br->offset += 8;
    }
    if(bits) {
      unsigned char bit;
      if(!bit_get(br, &bit)) return 0;
      result = (result << 1) | bit;
      bits--;
    }
  }
  *result_p = result;
  return 1;
}

static inline void bits_align(struct BitReader *br) {
  if(br->offset % 8 == 0) return;
  br->offset = ((br->offset >> 3)+1) << 3;
  // if(br->offset > br->size*8) {
  //   br->offset = br->size*8;
  // }
}

static inline unsigned bits_byte_offset(struct BitReader *br) {
  return br->offset / 8;
}


typedef struct leb128_byte {
  unsigned char flag:1;
  unsigned char payload:7;
} leb128_byte;

static inline int leb128_decode_unsigned(struct BitReader *br, uint64_t *result_p) {
  uint64_t result = 0;
  int shift = 0;
  unsigned char move_on = 1;

  if(br->offset & 0x7) {
    while(move_on) {
      if(!bit_get(br, &move_on)) return 0;
      uint64_t chunk;
      if(!bits_get(br, 7, &chunk)) return 0;
      result |= (chunk << shift);
      // fprintf(stderr, "ULeb: %d, %llu, %llu\r\n", move_on, chunk, result);
      shift += 7;
    }
  } else {
    while(move_on) {
      if(bits_remain(br) < 8) return 0;
      unsigned char b = *bits_head(br);
      move_on = b >> 7;
      result |= (b & 0x7F) << shift;
      shift += 7;
      bits_skip(br, 8);
    }
  }
  
  *result_p = result;
  return 1;
}

static inline int leb128_decode_signed(struct BitReader *br, int64_t *result_p) {
  int64_t result = 0;
  int shift = 0;
  unsigned char move_on = 1;
  unsigned char sign = 0;
  if(br->offset & 0x7) {
    while(move_on) {
      if(!bit_get(br, &move_on)) return 0;
      if(!bit_look(br, &sign)) return 0;
      uint64_t chunk;
      if(!bits_get(br, 7, &chunk)) return 0;
      result |= (chunk << shift);
      // fprintf(stderr, "SLeb: %d, %llu, %lld\r\n", move_on, chunk, sign ? result | - (1 << shift) :  result);
      shift += 7;
    }
  } else {
    while(move_on) {
      if(bits_remain(br) < 8) return 0;
      unsigned char b = *bits_head(br);
      move_on = b >> 7;
      sign = (b >> 6) & 1;
      result |= (b & 0x7F) << shift;
      shift += 7;
      bits_skip(br, 8);
    }
    
  }
  if(sign) {
    result |= - (1 << shift);
  }
  *result_p = result;
  return 1;
}


static int decode_delta(struct BitReader *br, int64_t *result, char flag) {
  if(flag) {
    return leb128_decode_signed(br, result);
  } else {
    *result = 0;
    return 1;
  }
}

static ERL_NIF_TERM
make_error(ErlNifEnv* env, const char *err) {
  return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, err));
}


static ERL_NIF_TERM
read_one_row(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary bin;
  unsigned int depth;
  if(!enif_inspect_binary(env, argv[0], &bin)) return make_error(env, "need_binary");
  if(!enif_get_uint(env, argv[1], &depth)) return make_error(env, "need_depth");

  unsigned shift = bin.size;

  struct BitReader br;
  bit_init(&br, bin.data, bin.size);
  
  uint64_t timestamp;
  
  const ERL_NIF_TERM *prev;

  char numbers_deltas[depth];
  int32_t numbers[depth];
  ERL_NIF_TERM row[depth];

  int i;
  
  ERL_NIF_TERM tag = enif_make_atom(env, "error");
  
  if(!bin.size) return make_error(env, "empty");
  
  
  int scale = 0;
  
  if(argc == 4) {
    if(!enif_get_int(env, argv[3], &scale)) make_error(env, "not_integer_scale");
  }
  
  unsigned char row_is_full;
  if(!bit_get(&br, &row_is_full)) return enif_make_badarg(env);
  
  
  if(row_is_full) {  // Decode full coded string    
    // fprintf(stderr, "Read full md %d\r\n", bin.size);
    
    if(bin.size < 8 + depth*4) {
      // fprintf(stderr, "Only %d bytes for depth %d, %d\r\n", (int)bin.size, (int)depth, (int)(8 + 2*2*depth*4));
      return make_error(env, "more_data_for_full_row");
    }
    
    // Here is decoding of simply coded full string
    tag = enif_make_atom(env, "row");
    if(!bits_get(&br, 63, &timestamp)) return make_error(env, "more_data_for_row_ts");
    
    for(i = 0; i < depth; i++) {
      bits_read32(&br, (int32_t *)&numbers[i]);
    }
  } else {
    tag = enif_make_atom(env, "delta_row");

    // fprintf(stderr, "Read delta md %d\r\n", bin.size);
    
    if(!bits_skip(&br, 3)) return make_error(env, "more_data_for_delta_flags");
    
    bzero(number_deltas, sizeof(number_deltas));
    
    for(i = 0; i < depth; i++) {
      unsigned char d;
      if(!bit_get(&br, &d)) return make_error(env, "more_data_for_delta_flags");
      number_deltas[i] = d;
    }
    
    bits_align(&br);
    
    if(!leb128_decode_unsigned(&br, (uint64_t *)&timestamp)) return make_error(env, "more_data_for_delta_ts");

    int64_t p,v;
    for(i = 0; i < depth; i++) {
      if(!decode_delta(&br, &p, number_deltas[i])) return make_error(env, "more_data_for_delta_number");
      numbers[i] = (int32_t)p;
    }    

    if(argc >= 3) {
      // And this means that we will append delta values to previous row
      tag = enif_make_atom(env, "row");
      int arity = 0;
      if(!enif_get_tuple(env, argv[2], &arity, &prev)) return make_error(env, "need_prev");
      if(arity != 4) return make_error(env, "need_prev_arity_4");

      ErlNifUInt64 prev_ts;
      if(!enif_get_uint64(env, prev[1], &prev_ts)) return make_error(env, "need_prev_ts");
      timestamp += prev_ts;
      
      ERL_NIF_TERM head, tail;
      
      // add previous values to bid
      
      // fprintf(stderr, "Apply delta to MD\r\n");
      
      tail = prev[2];
      for(i = 0; i < depth; i++) {
        if(!enif_get_list_cell(env, tail, &head, &tail)) return make_error(env, "need_more_numbers_in_prev");

        int number;
        if(!enif_get_int(env, head, &number)) return make_error(env, "need_number_in_prev");

        numbers[i] += number;
      }
      // fprintf(stderr, "\r\n\r\n\r\n");
    }
    
  }
  
  bits_align(&br);
  shift = bits_byte_offset(&br);
  
  // fprintf(stderr, "In the end: %d, %d, %d\r\n", bin.size, br.size, br.offset);

  
  double s = 1.0 / scale;
  for(i = 0; i < depth; i++) {
    row[i] = enif_make_int(env, numbers[i]);
  }
  
  return enif_make_tuple3(env,
    enif_make_atom(env, "ok"),
    enif_make_tuple3(env,
      tag,
      enif_make_uint64(env, timestamp),
      enif_make_list_from_array(env, row, depth)
    ),
    enif_make_uint64(env, shift)
    );
}


static int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

static int reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

static ErlNifFunc stockdb_funcs[] =
{
  {"do_decode_packet", 2, read_one_row},
  {"do_decode_packet", 4, read_one_row},
  {"do_decode_packet", 5, read_one_row}
};


ERL_NIF_INIT(stockdb_format, stockdb_funcs, NULL, reload, upgrade, NULL)
