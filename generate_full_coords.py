
def generate_scatter_coords(filename, stride=1):
    try:
        coords = ""
        with open(filename, 'r') as f:
            lines = f.readlines()
            
        for i, line in enumerate(lines):
            if i % stride == 0: 
                val = float(line.strip())
                coords += f"({i+1}, {val:.2f}) "
        return coords
    except Exception as e:
        return f"Error: {e}"

print(generate_scatter_coords(r"c:\Users\alext\Documents\Downloads\Лекционни материали-20260209\Coursework_Topic_31\topic31_wait_times.txt", stride=1))
