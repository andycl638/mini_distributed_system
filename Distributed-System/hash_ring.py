
from hashlib import md5
from array import array
import os
from struct import unpack_from

class Hash_ring(object):

    def __init__(self, partitioned, replicas, main_table, replica_table, total, node_count):

        self.partitioned = partitioned
        self.replicas = replicas
        self.main_table = main_table
        self.replica_table = replica_table
        self.total = total
        self.node_count = node_count

        print('main: ', self.main_table[23])
        print('replica: ', self.replica_table[6374] )
        print('total: ', self.total)

    def download_file(self, file):
        hash_shifted = unpack_from('>I', md5(file.encode('utf-8 ')).digest())[0] >> 16
        print('hash: ', hash_shifted)

        #get the disk and file from the main table
        main = self.main_table[hash_shifted]
        main_disk = main['disk']
        print('main: ', self.main_table[hash_shifted])

        #get the disk and file from the replica table
        replica = self.replica_table[hash_shifted]
        replica_disk = replica['disk']
        print('replica: ', self.replica_table[hash_shifted])

        disk_array = [main_disk, replica_disk]
        return disk_array

    def add_file(self, file):
        hash_shifted = unpack_from('>I', md5(file.encode('utf-8 ')).digest())[0] >> 16
        print('hash: ', hash_shifted)

        #finding disk based on shifted hash value
        main_disk = self.partitioned[hash_shifted]

        # add disk and file to the hash table with key as shifted hash value
        self.main_table[hash_shifted] = {'disk': main_disk, 'file': file}
        print('main: ', self.main_table[hash_shifted])

        if main_disk == 3:
            replica_disk = 0
        else:
            replica_disk = main_disk + 1
        # add disk and file to the hash table with key as shifted hash value
        self.replica_table[hash_shifted] = {'disk': replica_disk, 'file': file}
        print('replica: ', self.replica_table[hash_shifted])

        #return main and replication disk
        disk_array = [main_disk, replica_disk]
        return disk_array

    def add_partition(self, disk):
        print('Disk num ', disk)
        self.partitioned
        self.main_table
        self.replica_table

        partition = self.total / self.node_count
        print('Current partition: ', partition)
        new_size = self.node_count + 1
        redistribution = partition // new_size
        print('Recalcuated partition: ', redistribution)
        sum_partition = partition
        for x in range(self.node_count):
            print ('x: ', x)
            print('parititon: ', sum_partition)
            begin_slicing = int(sum_partition - redistribution)
            end_slicing = int(sum_partition)
            for idx in range(begin_slicing, end_slicing):
                self.partitioned[idx] = new_size-1
            sum_partition = sum_partition + partition
        print(self.partitioned)

    def remove_partition(self, disk):
        print('Disk num ', disk)
        print(type(disk))
        self.partitioned
        self.main_table
        self.replica_table

        partition = self.total / self.node_count
        print('Current partition: ', partition)
        new_size = self.node_count - 1
        redistribution = partition // new_size
        print('Recalcuated partition: ', redistribution)
        sum_partition = 0
        curr_disk = 0
        for x in range(self.node_count):
            print ('x: ', x)
            print('partition: ', sum_partition)
            begin_slicing = int(sum_partition)
            end_slicing = int(sum_partition+partition)
            print(self.partitioned[int(sum_partition)])
            if self.partitioned[int(sum_partition)] == disk:
                count = 0
                for curr_disk in range(new_size):
                    while count < redistribution:
                        self.partitioned[begin_slicing] = curr_disk
                        begin_slicing += 1
                        count += 1
                    count = 0
            sum_partition = sum_partition + partition
        print(self.partitioned)


    def get_main_file(self, disk_key):
        print('Disk Key: ', disk_key)
        file_list = []
        for key, value in self.main_table.items():
            if value.get('disk') == disk_key:
                file = value.get('file')
                print('File: ', file)
                file_list.append(file)
        print(file_list)

    def get_replica_file(self):
        print(self.replica_table)

def construct_ring(partition_power, replicas, node_count):
    total = 2 ** partition_power
    print ('total: ', total)
    partitioned = array('H')
    print (partitioned)

    for x in range(node_count):
        print ('x: ', x)
        partition_part = int(total / node_count)
        print('Partition: ', partition_part)
        count = 0
        for part in range(total):
            if (count < partition_part):
                partitioned.append(x)
            count += 1

    #creating main and replica tables
    main_table = {}
    replica_table = {}
    count = 0

    while len(main_table) < total:
        main_table[count] = {'disk': -1, 'file': ''}
        replica_table[count] = {'disk': -1, 'file': ''}
        count += 1

    ring = Hash_ring(partitioned, replicas, main_table, replica_table, total, node_count)
    print('Ring Created!')
    return ring

'''
ring = construct_ring(16, 2, 4)
print('-----------------')
print('-----------------')
print('ADD_FILE')
disk_add = ring.add_file('hi.txt')
print('disks: ', disk_add)
print('-----------------')
print('-----------------')
print('DOWNLOAD_FILE')
disk_download = ring.download_file('hi.txt')
print('disks: ', disk_download)

print('Main Table')
ring.print_main()
print('Replica table')
#ring.print_replica()'''
