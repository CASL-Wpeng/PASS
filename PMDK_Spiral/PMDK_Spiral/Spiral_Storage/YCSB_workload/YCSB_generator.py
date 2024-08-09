
op_number = 1000

operation = []
key = []

# with open('./rw-50-50-load-pure.txt','r') as file: 
#     for line in file:
#         if "INSERT" == line[0:6]:
#             operation.append("INSERT")
#             key.append(line[21:40])

# file_to_write=open("./rw-50-50-load.txt","w")
# for i in range(0,op_number):
#     file_to_write.write(operation[i] + " "+ key[i] + "\n")
# file_to_write.close

operation = []
key = []
with open('./rw-50-50-run-pure.txt','r') as file: 
    for line in file:
        if "UPDATE" == line[0:6]:
            operation.append("UPDATE")
            key.append(line[21:40])
        if "READ" == line[0:4]:
            operation.append("READ")
            key.append(line[19:38])

file_to_write=open("./rw-50-50-run.txt","w")
for i in range(0,op_number):
    file_to_write.write(operation[i] + " "+ key[i] + "\n")
file_to_write.close
