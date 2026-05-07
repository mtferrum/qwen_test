; ==============================================================================
; Platform Compiler - x86_64 Assembly Implementation
; ==============================================================================
; This is a demonstration/educational implementation showing how core compiler
; logic could be expressed in assembly. In practice, this would interface with
; C libraries for complex operations (file I/O, regex, YAML parsing).
;
; Architecture: x86_64 Linux (System V AMD64 ABI)
; Assembler: NASM or GAS syntax
; ==============================================================================

section .data
    ; Compiler version and metadata
    compiler_version db "Platform Compiler ASM v1.0.0", 0
    newline db 10
    
    ; Error messages
    err_file_not_found db "Error: File not found: ", 0
    err_parse_error db "Error: Parse error at line ", 0
    err_invalid_syntax db "Error: Invalid syntax", 0
    err_unsupported_platform db "Error: Unsupported platform: ", 0
    
    ; Success messages
    msg_parsing db "Parsing DSL...", 10, 0
    msg_loading_config db "Loading configuration...", 10, 0
    msg_generating db "Generating code...", 10, 0
    msg_written db "  Written: ", 0
    
    ; Keywords for DSL parsing
    keyword_define_table db "DEFINE TABLE", 0
    keyword_define_model db "DEFINE MODEL", 0
    keyword_define_graph db "DEFINE GRAPH", 0
    keyword_create_view db "CREATE VIEW", 0
    keyword_insert_into db "INSERT INTO", 0
    keyword_with_stream db "WITH STREAM", 0
    
    ; Platform names
    platform_spark db "spark", 0
    platform_flink db "flink", 0
    platform_yql db "yql", 0
    
    ; Output file templates
    sql_extension db ".sql", 0
    dag_prefix db "dag_", 0
    deployment_prefix db "deployment_", 0
    yaml_extension db ".yaml", 0
    py_extension db ".py", 0

section .bss
    ; Global compiler state
    compiler_state resq 1           ; Pointer to compiler state struct
    
    ; Parser state
    parser_tables_ptr resq 1        ; Array of table definitions
    parser_models_ptr resq 1        ; Array of model definitions
    parser_graphs_ptr resq 1        ; Array of graph definitions
    parser_views_ptr resq 1         ; Hash map of views
    parser_inserts_ptr resq 1       ; Array of insert statements
    parser_table_count resd 1       ; Number of tables parsed
    parser_model_count resd 1       ; Number of models parsed
    parser_graph_count resd 1       ; Number of graphs parsed
    parser_view_count resd 1        ; Number of views parsed
    parser_insert_count resd 1      ; Number of inserts parsed
    
    ; Config loader state
    config_platform resb 32         ; Target platform name
    config_mode resb 32             ; Execution mode (batch/streaming)
    config_parallelism resd 1       ; Parallelism level
    config_memory_driver resb 16    ; Driver memory
    config_memory_executor resb 16  ; Executor memory
    
    ; Buffer for file reading
    file_buffer resb 65536          ; 64KB read buffer
    file_size resq 1                ; Size of loaded file
    
    ; Working buffers
    temp_buffer resb 4096           ; Temporary string buffer
    output_buffer resb 131072       ; 128KB output buffer

section .text
    global main
    extern printf, malloc, free, fopen, fread, fclose, fwrite
    extern strlen, strcpy, strcat, strcmp, strstr, strtok
    extern memset, memcpy

; ==============================================================================
; Main Entry Point (C-compatible)
; int main(int argc, char** argv)
; ==============================================================================
main:
    ; Set up stack frame
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    push r14
    push r15
    
    ; Save argc and argv
    mov r12, rdi            ; argc
    mov r13, rsi            ; argv
    
    ; Initialize compiler state
    call init_compiler
    
    ; Check command line arguments
    cmp rdi, 4              ; Expecting: program dsl_file config_file output_dir
    jl .usage_error
    
    ; Get arguments from argv
    mov rax, [r13 + 8]      ; argv[1] = dsl_path
    mov rbx, [r13 + 16]     ; argv[2] = config_path
    mov rcx, [r13 + 24]     ; argv[3] = output_dir
    
    ; Run compilation
    mov rdi, rax            ; dsl_path
    mov rsi, rbx            ; config_path
    mov rdx, rcx            ; output_dir
    call compile_pipeline
    
    ; Exit successfully
    xor eax, eax            ; return 0
    
    jmp .cleanup_main

.usage_error:
    ; Print usage message
    lea rdi, [rel usage_msg]
    call printf
    
    mov eax, 1              ; return 1 (error)
    
.cleanup_main:
    ; Cleanup and restore registers
    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

; ==============================================================================
; Usage Message
; ==============================================================================
section .data
    usage_msg db "Usage: platform_compiler_asm <dsl_file> <config_file> <output_dir>", 10
    usage_msg_len equ $ - usage_msg

section .text

; ==============================================================================
; Initialize Compiler State
; ==============================================================================
; Allocates and initializes the compiler state structure
init_compiler:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    push r14
    push r15
    
    ; Allocate memory for compiler state (256 bytes)
    mov rdi, 256
    call malloc
    test rax, rax
    jz .alloc_failed
    
    ; Store pointer to global state
    mov [rel compiler_state], rax
    
    ; Zero out the state
    mov rdi, rax
    xor esi, esi
    mov ecx, 256
    rep stosb
    
    ; Initialize parser state pointers to NULL
    mov rax, [rel compiler_state]
    xor rdx, rdx
    mov [rax + 0], rdx      ; tables_ptr = NULL
    mov [rax + 8], rdx      ; models_ptr = NULL
    mov [rax + 16], rdx     ; graphs_ptr = NULL
    mov [rax + 24], rdx     ; views_ptr = NULL
    mov [rax + 32], rdx     ; inserts_ptr = NULL
    
    ; Initialize counts to 0
    mov dword [rax + 40], 0 ; table_count
    mov dword [rax + 44], 0 ; model_count
    mov dword [rax + 48], 0 ; graph_count
    mov dword [rax + 52], 0 ; view_count
    mov dword [rax + 56], 0 ; insert_count
    
    ; Print initialization message
    lea rdi, [rel compiler_version]
    call printf
    
    jmp .done

.alloc_failed:
    ; Memory allocation failed
    mov rax, 60
    mov edi, 2
    syscall

.done:
    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

; ==============================================================================
; Main Compile Pipeline Function
; ==============================================================================
; Arguments:
;   rdi = dsl_path (char*)
;   rsi = config_path (char*)
;   rdx = output_dir (char*)
compile_pipeline:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    push r14
    push r15
    
    ; Save arguments
    mov r12, rdi            ; dsl_path
    mov r13, rsi            ; config_path
    mov r14, rdx            ; output_dir
    
    ; Step 1: Parse DSL file
    lea rdi, [rel msg_parsing]
    call printf
    
    mov rdi, r12            ; dsl_path
    call parse_dsl_file
    test eax, eax
    js .parse_error
    
    ; Step 2: Load configuration
    lea rdi, [rel msg_loading_config]
    call printf
    
    mov rdi, r13            ; config_path
    call load_config_file
    test eax, eax
    js .config_error
    
    ; Step 3: Validate configuration
    call validate_config
    test eax, eax
    js .validation_error
    
    ; Step 4: Select generator based on platform
    call get_platform_name
    mov r15, rax            ; platform name string
    
    ; Compare with "spark"
    lea rdi, [rel platform_spark]
    mov rsi, r15
    call strcmp
    test eax, eax
    je .use_spark_generator
    
    ; Compare with "flink"
    lea rdi, [rel platform_flink]
    mov rsi, r15
    call strcmp
    test eax, eax
    je .use_flink_generator
    
    ; Unsupported platform
    lea rdi, [rel err_unsupported_platform]
    call printf
    mov eax, -4
    jmp .error

.use_spark_generator:
    lea rdi, [rel msg_generating]
    call printf
    call generate_spark_code
    jmp .generate_done

.use_flink_generator:
    lea rdi, [rel msg_generating]
    call printf
    call generate_flink_code
    jmp .generate_done

.generate_done:
    ; Write output files
    mov rdi, r14            ; output_dir
    call write_output_files
    
    xor eax, eax            ; return 0 (success)
    jmp .cleanup

.parse_error:
    lea rdi, [rel err_parse_error]
    call printf
    mov eax, -1
    jmp .error

.config_error:
    lea rdi, [rel err_file_not_found]
    call printf
    mov eax, -2
    jmp .error

.validation_error:
    lea rdi, [rel err_invalid_syntax]
    call printf
    mov eax, -3

.error:
    ; Error cleanup
    call cleanup_compiler

.cleanup:
    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

; ==============================================================================
; Parse DSL File
; ==============================================================================
; Arguments:
;   rdi = file_path (char*)
; Returns:
;   eax = 0 on success, negative on error
parse_dsl_file:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    push r14
    push r15
    
    mov r12, rdi            ; Save file path
    
    ; Open file for reading
    mov rdi, r12
    lea rsi, [rel read_mode]
    call fopen
    test rax, rax
    jz .file_open_error
    
    mov r13, rax            ; Save FILE* pointer
    
    ; Read entire file into buffer
    mov rdi, r13            ; FILE*
    lea rsi, [rel file_buffer]
    mov edx, 65535          ; Max bytes to read
    call fread
    mov [rel file_size], rax
    
    ; Null-terminate the buffer
    lea rdx, [rel file_buffer]
    add rdx, rax
    mov byte [rdx], 0
    
    ; Close file
    mov rdi, r13
    call fclose
    
    ; Remove comments from content
    lea rdi, [rel file_buffer]
    call remove_comments
    
    ; Parse DEFINE TABLE statements
    lea rdi, [rel file_buffer]
    call parse_define_tables
    
    ; Parse DEFINE MODEL statements
    lea rdi, [rel file_buffer]
    call parse_define_models
    
    ; Parse DEFINE GRAPH statements
    lea rdi, [rel file_buffer]
    call parse_define_graphs
    
    ; Parse CREATE VIEW statements
    lea rdi, [rel file_buffer]
    call parse_create_views
    
    ; Parse INSERT INTO statements
    lea rdi, [rel file_buffer]
    call parse_insert_statements
    
    xor eax, eax            ; Return success
    jmp .done

.file_open_error:
    mov eax, -1

.done:
    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

section .data
    read_mode db "r", 0

section .text

; ==============================================================================
; Remove Comments from DSL Content
; ==============================================================================
; Arguments:
;   rdi = content (char*) - modified in place
remove_comments:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    
    mov r12, rdi            ; Source pointer
    mov r13, rdi            ; Destination pointer (same buffer)
    
.remove_loop:
    movzx eax, byte [r12]
    test al, al             ; End of string?
    jz .done
    
    ; Check for single-line comment (--)
    cmp byte [r12], '-'
    jne .check_multiline
    cmp byte [r12 + 1], '-'
    jne .check_multiline
    
    ; Skip until end of line
.skip_line_comment:
    movzx eax, byte [r12]
    test al, al
    jz .done
    cmp al, 10              ; Newline?
    inc r12
    jne .skip_line_comment
    ; Copy the newline
    mov byte [r13], 10
    inc r13
    jmp .remove_loop
    
.check_multiline:
    ; Check for multi-line comment (/*)
    cmp byte [r12], '/'
    jne .copy_char
    cmp byte [r12 + 1], '*'
    jne .copy_char
    
    ; Skip until */
.skip_block_comment:
    movzx eax, byte [r12]
    test al, al
    jz .done
    cmp al, '*'
    jne .skip_next_char
    cmp byte [r12 + 1], '/'
    jne .skip_next_char
    add r12, 2              ; Skip */
    jmp .remove_loop
    
.skip_next_char:
    inc r12
    jmp .skip_block_comment
    
.copy_char:
    ; Copy character to destination
    movzx eax, byte [r12]
    mov byte [r13], al
    inc r12
    inc r13
    jmp .remove_loop

.done:
    ; Null-terminate result
    mov byte [r13], 0
    
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

; ==============================================================================
; Parse DEFINE TABLE Statements
; ==============================================================================
; Arguments:
;   rdi = content (char*)
parse_define_tables:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    push r14
    
    mov r12, rdi            ; Content pointer
    lea r13, [rel keyword_define_table]
    
.search_loop:
    ; Search for "DEFINE TABLE"
    mov rdi, r12
    mov rsi, r13
    call strstr
    test rax, rax
    jz .done
    
    mov r12, rax            ; Found position
    
    ; Extract table name (after "DEFINE TABLE ")
    add r12, 12             ; Length of "DEFINE TABLE"
    
    ; Skip whitespace
.skip_ws:
    movzx eax, byte [r12]
    cmp al, ' '
    je .next_ws
    cmp al, 9               ; Tab
    je .next_ws
    cmp al, 10              ; Newline
    je .next_ws
    jmp .found_name_start
    
.next_ws:
    inc r12
    jmp .skip_ws
    
.found_name_start:
    mov r14, r12            ; Start of table name
    
    ; Find end of name (whitespace or '(')
.find_name_end:
    movzx eax, byte [r12]
    cmp al, ' '
    je .found_name_end
    cmp al, 9
    je .found_name_end
    cmp al, 10
    je .found_name_end
    cmp al, '('
    je .found_name_end
    test al, al
    jz .found_name_end
    inc r12
    jmp .find_name_end
    
.found_name_end:
    ; r14 = start, r12 = end of table name
    ; Store table name (simplified - just count for now)
    inc dword [rel parser_table_count]
    
    ; Continue searching for more tables
    jmp .search_loop

.done:
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

; ==============================================================================
; Parse DEFINE MODEL Statements
; ==============================================================================
parse_define_models:
    push rbp
    mov rbp, rsp
    ; Simplified - similar structure to parse_define_tables
    inc dword [rel parser_model_count]
    pop rbp
    ret

; ==============================================================================
; Parse DEFINE GRAPH Statements
; ==============================================================================
parse_define_graphs:
    push rbp
    mov rbp, rsp
    inc dword [rel parser_graph_count]
    pop rbp
    ret

; ==============================================================================
; Parse CREATE VIEW Statements
; ==============================================================================
parse_create_views:
    push rbp
    mov rbp, rsp
    inc dword [rel parser_view_count]
    pop rbp
    ret

; ==============================================================================
; Parse INSERT INTO Statements
; ==============================================================================
parse_insert_statements:
    push rbp
    mov rbp, rsp
    inc dword [rel parser_insert_count]
    pop rbp
    ret

; ==============================================================================
; Load Configuration File
; ==============================================================================
; Arguments:
;   rdi = config_path (char*)
; Returns:
;   eax = 0 on success, negative on error
load_config_file:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    
    mov r12, rdi
    
    ; Open file
    mov rdi, r12
    lea rsi, [rel read_mode]
    call fopen
    test rax, rax
    jz .error
    
    mov rbx, rax            ; Save FILE*
    
    ; Read file
    mov rdi, rbx
    lea rsi, [rel file_buffer]
    mov edx, 65535
    call fread
    
    ; Close file
    mov rdi, rbx
    call fclose
    
    ; Parse YAML (simplified - extract key fields)
    lea rdi, [rel file_buffer]
    call parse_yaml_config
    
    xor eax, eax
    jmp .done

.error:
    mov eax, -1

.done:
    pop r12
    pop rbx
    pop rbp
    ret

; ==============================================================================
; Parse YAML Configuration
; ==============================================================================
parse_yaml_config:
    push rbp
    mov rbp, rsp
    
    ; Search for "platform:" key
    lea rdi, [rel file_buffer]
    lea rsi, [rel yaml_platform_key]
    call strstr
    test rax, rax
    jz .no_platform
    
    ; Extract platform value
    ; (simplified - copy to config_platform buffer)
    
.no_platform:
    ; Search for "mode:" key
    lea rdi, [rel file_buffer]
    lea rsi, [rel yaml_mode_key]
    call strstr
    
    pop rbp
    ret

section .data
    yaml_platform_key db "platform:", 0
    yaml_mode_key db "mode:", 0

section .text

; ==============================================================================
; Validate Configuration
; ==============================================================================
validate_config:
    push rbp
    mov rbp, rsp
    
    ; Check platform is valid
    lea rdi, [rel config_platform]
    lea rsi, [rel platform_spark]
    call strcmp
    test eax, eax
    je .valid_spark
    
    lea rdi, [rel config_platform]
    lea rsi, [rel platform_flink]
    call strcmp
    test eax, eax
    je .valid_flink
    
    lea rdi, [rel config_platform]
    lea rsi, [rel platform_yql]
    call strcmp
    test eax, eax
    je .valid_yql
    
    ; Invalid platform
    mov eax, -1
    jmp .done

.valid_spark:
.valid_flink:
.valid_yql:
    ; Check mode is valid (batch or streaming)
    ; Check Flink streaming requires checkpointing
    ; (additional validation logic here)
    
    xor eax, eax            ; Valid

.done:
    pop rbp
    ret

; ==============================================================================
; Get Platform Name
; ==============================================================================
get_platform_name:
    push rbp
    mov rbp, rsp
    lea rax, [rel config_platform]
    pop rbp
    ret

; ==============================================================================
; Generate Spark Code
; ==============================================================================
generate_spark_code:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    
    ; Clear output buffer
    lea rdi, [rel output_buffer]
    xor esi, esi
    mov ecx, 131072
    rep stosb
    
    ; Generate header
    lea rdi, [rel output_buffer]
    call generate_spark_header
    
    ; Generate source DDL
    lea rdi, [rel output_buffer]
    call generate_spark_source_ddl
    
    ; Generate sink DDL
    lea rdi, [rel output_buffer]
    call generate_spark_sink_ddl
    
    ; Generate transformations
    lea rdi, [rel output_buffer]
    call generate_spark_transformations
    
    ; Generate UDF registrations
    lea rdi, [rel output_buffer]
    call generate_spark_udf
    
    ; Generate footer
    lea rdi, [rel output_buffer]
    call generate_spark_footer
    
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

; ==============================================================================
; Generate Spark Header
; ==============================================================================
generate_spark_header:
    push rbp
    mov rbp, rsp
    push rbx
    
    mov rdi, rdi            ; Output buffer
    lea rsi, [rel spark_header_template]
    call strcat
    
    pop rbx
    pop rbp
    ret

section .data
    spark_header_template db "-- ==========================================", 10
                            db "-- Generated by Platform Compiler ASM", 10
                            db "-- Target: SPARK", 10
                            db "-- ==========================================", 10, 10, 0

section .text

; ==============================================================================
; Generate Spark Source DDL
; ==============================================================================
generate_spark_source_ddl:
    push rbp
    mov rbp, rsp
    
    ; Add section header
    lea rdi, [rel output_buffer]
    lea rsi, [rel spark_source_header]
    call strcat
    
    ; Iterate through parsed tables and generate DDL
    ; (simplified - would loop through parser_tables_ptr)
    
    pop rbp
    ret

section .data
    spark_source_header db 10, "-- ==========================================", 10
                         db "-- SOURCE TABLES", 10
                         db "-- ==========================================", 10, 0

section .text

; ==============================================================================
; Generate Spark Sink DDL
; ==============================================================================
generate_spark_sink_ddl:
    push rbp
    mov rbp, rsp
    
    lea rdi, [rel output_buffer]
    lea rsi, [rel spark_sink_header]
    call strcat
    
    pop rbp
    ret

section .data
    spark_sink_header db 10, "-- ==========================================", 10
                       db "-- SINK TABLES", 10
                       db "-- ==========================================", 10, 0

section .text

; ==============================================================================
; Generate Spark Transformations
; ==============================================================================
generate_spark_transformations:
    push rbp
    mov rbp, rsp
    
    lea rdi, [rel output_buffer]
    lea rsi, [rel spark_transform_header]
    call strcat
    
    ; Iterate through views and generate CREATE VIEW statements
    ; Translate DSL functions to Spark SQL equivalents
    
    pop rbp
    ret

section .data
    spark_transform_header db 10, "-- ==========================================", 10
                            db "-- TRANSFORMATIONS", 10
                            db "-- ==========================================", 10, 0

section .text

; ==============================================================================
; Generate Spark UDF Registrations
; ==============================================================================
generate_spark_udf:
    push rbp
    mov rbp, rsp
    
    lea rdi, [rel output_buffer]
    lea rsi, [rel spark_udf_header]
    call strcat
    
    pop rbp
    ret

section .data
    spark_udf_header db 10, "-- ==========================================", 10
                      db "-- UDF/UDTF REGISTRATIONS", 10
                      db "-- ==========================================", 10, 0

section .text

; ==============================================================================
; Generate Spark Footer
; ==============================================================================
generate_spark_footer:
    push rbp
    mov rbp, rsp
    
    lea rdi, [rel output_buffer]
    lea rsi, [rel spark_footer]
    call strcat
    
    pop rbp
    ret

section .data
    spark_footer db 10, "-- End of generated script", 10, 0

section .text

; ==============================================================================
; Generate Flink Code
; ==============================================================================
generate_flink_code:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    
    ; Clear output buffer
    lea rdi, [rel output_buffer]
    xor esi, esi
    mov ecx, 131072
    rep stosb
    
    ; Generate header
    lea rdi, [rel output_buffer]
    lea rsi, [rel flink_header_template]
    call strcat
    
    ; Generate source DDL (Flink CREATE TABLE with WATERMARK)
    lea rdi, [rel output_buffer]
    call generate_flink_source_ddl
    
    ; Generate sink DDL
    lea rdi, [rel output_buffer]
    call generate_flink_sink_ddl
    
    ; Generate transformations
    lea rdi, [rel output_buffer]
    call generate_flink_transformations
    
    ; Generate K8s manifest if needed
    call check_kubernetes_orchestration
    test eax, eax
    jz .skip_k8s
    call generate_k8s_manifest
    
.skip_k8s:
    ; Generate footer
    lea rdi, [rel output_buffer]
    lea rsi, [rel flink_footer]
    call strcat
    
    pop r12
    pop rbx
    pop rbp
    ret

section .data
    flink_header_template db "-- ==========================================", 10
                           db "-- Generated by Platform Compiler ASM", 10
                           db "-- Target: FLINK", 10
                           db "-- Mode: STREAMING", 10
                           db "-- ==========================================", 10, 10, 0
    flink_footer db 10, "-- End of generated script", 10, 0

section .text

; ==============================================================================
; Generate Flink Source DDL
; ==============================================================================
generate_flink_source_ddl:
    push rbp
    mov rbp, rsp
    
    lea rdi, [rel output_buffer]
    lea rsi, [rel flink_source_header]
    call strcat
    
    ; For each table with stream config, generate CREATE TABLE with WATERMARK
    ; Example:
    ; CREATE TABLE events (
    ;   event_id STRING,
    ;   user_id STRING,
    ;   event_time TIMESTAMP(3),
    ;   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ; ) WITH (
    ;   'connector' = 'kafka',
    ;   ...
    ; );
    
    pop rbp
    ret

section .data
    flink_source_header db 10, "-- ==========================================", 10
                         db "-- SOURCE TABLES", 10
                         db "-- ==========================================", 10, 0

section .text

; ==============================================================================
; Generate Flink Sink DDL
; ==============================================================================
generate_flink_sink_ddl:
    push rbp
    mov rbp, rsp
    
    lea rdi, [rel output_buffer]
    lea rsi, [rel flink_sink_header]
    call strcat
    
    pop rbp
    ret

section .data
    flink_sink_header db 10, "-- ==========================================", 10
                        db "-- SINK TABLES", 10
                        db "-- ==========================================", 10, 0

section .text

; ==============================================================================
; Generate Flink Transformations
; ==============================================================================
generate_flink_transformations:
    push rbp
    mov rbp, rsp
    
    lea rdi, [rel output_buffer]
    lea rsi, [rel flink_transform_header]
    call strcat
    
    ; Generate CREATE VIEW and INSERT INTO statements
    ; Flink SQL supports TUMBLE, HOP, SESSION window functions natively
    
    pop rbp
    ret

section .data
    flink_transform_header db 10, "-- ==========================================", 10
                            db "-- TRANSFORMATIONS", 10
                            db "-- ==========================================", 10, 0

section .text

; ==============================================================================
; Check Kubernetes Orchestration
; ==============================================================================
check_kubernetes_orchestration:
    push rbp
    mov rbp, rsp
    
    ; Check if orchestration type is kubernetes
    ; Return 1 if yes, 0 if no
    mov eax, 1              ; Simplified - assume k8s for demo
    
    pop rbp
    ret

; ==============================================================================
; Generate Kubernetes Manifest
; ==============================================================================
generate_k8s_manifest:
    push rbp
    mov rbp, rsp
    
    ; Generate FlinkDeployment YAML manifest
    ; apiVersion: flink.apache.org/v1beta1
    ; kind: FlinkDeployment
    ; metadata:
    ;   name: <pipeline-name>
    ; spec:
    ;   image: <flink-image>
    ;   flinkConfiguration:
    ;     taskmanager.numberOfTaskSlots: <parallelism>
    ;     state.backend: rocksdb
    ;     ...
    
    pop rbp
    ret

; ==============================================================================
; Write Output Files
; ==============================================================================
; Arguments:
;   rdi = output_dir (char*)
write_output_files:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    push r14
    
    mov r12, rdi            ; output_dir
    
    ; Build output file path
    lea rdi, [rel temp_buffer]
    mov rsi, r12            ; output_dir
    call strcpy
    
    ; Add trailing slash if needed
    ; Add filename based on platform
    
    ; Write SQL file
    lea rdi, [rel temp_buffer]
    lea rsi, [rel output_buffer]
    call write_file
    
    ; Generate and write orchestration artifacts (DAG or K8s manifest)
    call check_platform_and_generate_artifacts
    
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

; ==============================================================================
; Write File Helper
; ==============================================================================
; Arguments:
;   rdi = file_path (char*)
;   rsi = content (char*)
write_file:
    push rbp
    mov rbp, rsp
    push rbx
    push r12
    push r13
    
    mov r12, rdi            ; file_path
    mov r13, rsi            ; content
    
    ; Open file for writing
    mov rdi, r12
    lea rsi, [rel write_mode]
    call fopen
    test rax, rax
    jz .error
    
    mov rbx, rax            ; Save FILE*
    
    ; Get content length
    mov rdi, r13
    call strlen
    mov r14, rax            ; content length
    
    ; Write content
    mov rdi, rbx
    mov rsi, r13
    mov rdx, r14
    call fwrite
    
    ; Close file
    mov rdi, rbx
    call fclose
    
    ; Print written message
    lea rdi, [rel msg_written]
    call printf
    mov rdi, r12
    call printf
    mov rdi, [rel newline_ptr]
    call printf
    
    xor eax, eax
    jmp .done

.error:
    mov eax, -1

.done:
    pop r13
    pop r12
    pop rbx
    pop rbp
    ret

section .data
    write_mode db "w", 0
    newline_ptr dq newline

section .text

; ==============================================================================
; Check Platform and Generate Artifacts
; ==============================================================================
check_platform_and_generate_artifacts:
    push rbp
    mov rbp, rsp
    
    ; Check platform and generate appropriate orchestration artifacts:
    ; - Spark + Airflow -> DAG Python file
    ; - Flink + Kubernetes -> Deployment YAML
    
    pop rbp
    ret

; ==============================================================================
; Cleanup Compiler State
; ==============================================================================
cleanup_compiler:
    push rbp
    mov rbp, rsp
    
    ; Free allocated memory
    mov rax, [rel compiler_state]
    test rax, rax
    jz .done
    
    ; Free parser arrays if allocated
    ; ...
    
    ; Free compiler state
    mov rdi, rax
    call free
    xor rax, rax
    mov [rel compiler_state], rax

.done:
    pop rbp
    ret

; ==============================================================================
; Utility Functions
; ==============================================================================

; String comparison wrapper
; Already using external strcmp

; Memory operations
; Already using external memset, memcpy

; ==============================================================================
; End of Program
; ==============================================================================
section .data
    end_marker db "END_OF_PROGRAM", 0

section .text
    global __bss_start
    global _edata
    global _end
