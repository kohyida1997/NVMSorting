

typedef struct BYTE_24 { unsigned char val[24]; } BYTE_24;


struct Record {
    uint64_t key;
    BYTE_24 value;
};
