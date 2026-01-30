#include <assert.h>
#include <cerrno>
#include <cstdint>
#include <stdio.h>
#include <string.h>

// SPIR-V to C header converter
// Unlike bin2c, this outputs uint32_t array (SPIR-V is always 32-bit aligned)
// and includes the size in 32-bit words

int main(int argc, char** argv)
{
    if (argc != 4) {
        fprintf(stderr, "Usage: %s \"namespace\" \"constant_name\" \"input_filename\"\n", argv[0]);
        return -1;
    }
    char* fn = argv[3];
    FILE* f  = fopen(fn, "rb");

    if (f == nullptr) {
        fprintf(stderr, "Error opening file: %s\n", strerror(errno));
        return -1;
    }

    // Get file size
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size % 4 != 0) {
        fprintf(stderr, "Error: SPIR-V file size must be multiple of 4 bytes\n");
        fclose(f);
        return -1;
    }

    long word_count = file_size / 4;

    // Add include for uint32_t
    printf("#include <cstdint>\n\n");

    int   count = 0;
    char* pch   = strtok(argv[1], "::");
    while (pch != nullptr) {
        printf("namespace %s {\n", pch);
        pch = strtok(nullptr, "::");
        count++;
    }

    printf("const uint32_t %s[] = {\n", argv[2]);
    unsigned long n = 0;
    while (!feof(f)) {
        uint32_t word;
        if (fread(&word, 4, 1, f) == 0)
            break;
        printf("0x%.8X,", word);
        ++n;
        if (n % 8 == 0)
            printf("\n");
    }
    fclose(f);
    printf("\n};\n");
    printf("const size_t %s_size = %ld;\n", argv[2], word_count);

    for (int i = 0; i < count; i++)
        printf("}\n");

    return 0;
}
