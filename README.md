# Problem :
   To harness the benefits of distributed processing, it's essential to compute splits for binary variable-length files. 
Without generating splits, all processing would be confined to a single machine.

   This code specifically targets mainframe binary files, characterized by a Read Descriptor Word (RDW) of 4 bytes. The RDW 
indicates the length of the subsequent data. By processing the file in two passes, we first create splits, and then in the
second pass, we conduct the actual processing, utilizing multiple executors effectively.
