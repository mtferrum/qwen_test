#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char** argv) {
    if (argc < 4) {
        printf("Usage: %s <dsl_file> <config_file> <output_dir>\n", argv[0]);
        return 1;
    }
    
    printf("DSL file: %s\n", argv[1]);
    printf("Config file: %s\n", argv[2]);
    printf("Output dir: %s\n", argv[3]);
    
    // Try to open DSL file
    FILE* f = fopen(argv[1], "r");
    if (!f) {
        printf("Error: Cannot open %s\n", argv[1]);
        return 1;
    }
    
    char buffer[65536];
    size_t bytes = fread(buffer, 1, sizeof(buffer)-1, f);
    buffer[bytes] = '\0';
    fclose(f);
    
    printf("Read %zu bytes from DSL file\n", bytes);
    
    // Count DEFINE TABLE occurrences
    int count = 0;
    char* p = buffer;
    while ((p = strstr(p, "DEFINE TABLE")) != NULL) {
        count++;
        p += 12;
    }
    printf("Found %d DEFINE TABLE statements\n", count);
    
    return 0;
}
