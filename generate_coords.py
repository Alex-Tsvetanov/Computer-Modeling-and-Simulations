import numpy as np

def generate_histogram_coords(filename, bins=10):
    try:
        data = []
        with open(filename, 'r') as f:
            for line in f:
                data.append(float(line.strip()))
        
        if not data:
            return ""
            
        hist, bin_edges = np.histogram(data, bins=bins)
        
        coords = ""
        for i in range(len(hist)):
            # x is center of bin
            x = (bin_edges[i] + bin_edges[i+1]) / 2
            y = hist[i]
            coords += f"({x:.1f}, {y}) "
        return coords
    except Exception as e:
        return f"Error: {e}"

def generate_scatter_coords(filename, stride=10):
    try:
        coords = ""
        with open(filename, 'r') as f:
            lines = f.readlines()
            
        for i, line in enumerate(lines):
            if i % stride == 0: # Take every nth point to avoid overcrowding
                val = float(line.strip())
                coords += f"({i+1}, {val:.2f}) "
        return coords
    except Exception as e:
        return f"Error: {e}"

print("Topic 5 Coords:")
coords5 = generate_histogram_coords(r"c:\Users\alext\Documents\Downloads\Лекционни материали-20260209\Coursework_Topic_5\wait_times_1.txt")
print(coords5)

print("\nTopic 31 Coords:")
coords31 = generate_scatter_coords(r"c:\Users\alext\Documents\Downloads\Лекционни материали-20260209\Coursework_Topic_31\topic31_wait_times.txt", stride=5)
print(coords31)

with open(r"c:\Users\alext\Documents\Downloads\Лекционни материали-20260209\coords.txt", "w") as f:
    f.write("TOPIC 5:\n")
    f.write(coords5)
    f.write("\nTOPIC 31:\n")
    f.write(coords31)

with open(r"c:\Users\alext\Documents\Downloads\Лекционни материали-20260209\coords.txt", "w") as f:
    f.write("TOPIC 5:\n")
    f.write(coords5)
    f.write("\nTOPIC 31:\n")
    f.write(coords31)
