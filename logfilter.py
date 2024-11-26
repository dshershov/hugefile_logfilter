import gc

def find_sequence_and_copy(file_path, sequence, output_path, read_block_size=128 * 128*2, chunk_size=1 * 1024**3):
    sequence = sequence.encode('ascii')
    sequence_length = len(sequence)
    total_read = 0
    buffer = b''
    found_position = -1
    inc = 0
    some = True
    with open(file_path, 'rb') as f:
        while some==True:
            data = f.read(read_block_size)
            inc = inc + 1
            if (inc % 10)==0:
                print("MB was reading", inc*5)

            if not data:
                break  # EOF
            
            buffer += data
            # Try to find position in buffer
            position = buffer.find(sequence)
            if position != -1:
                found_position = total_read + position
                print(f"Found position here {found_position}")
                some=False
                #break
            
            total_read += len(data)
            buffer = buffer[-sequence_length:]
            buffer = b''
            del data
            gc.collect()

    if found_position == -1:
        print("Position not found")
        return

    with open(output_path, 'wb') as out_f:
        with open(file_path, 'rb') as f1:
            f1.seek(found_position-200)
            remaining = chunk_size  

            while remaining > 0:
                block = f1.read(min(read_block_size, remaining))
                if not block:
                    break
                
                out_f.write(block)
                remaining -= len(block)
                del block
                gc.collect()

            print(f"Data was here: {output_path}")

# Example for running
file_path = "/log/s3/trace-topic/trace.log"
sequence = "2024-01-10"
output_path = "/log/s3/trace-topic/trace.logfiltred.log"

find_sequence_and_copy(file_path, sequence , output_path)
