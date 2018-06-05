import sys
import socket
from threading import Thread
import os
import subprocess
import hash_ring
import shlex
from hash_ring import construct_ring

def check_ip(addr):
    try:
        socket.inet_aton(addr)
        print ('Valid IP address: %s' % addr)
        return True
    except socket.error:
        print ('Not an IP address: %s' % addr)
        return False

# Get ip address for host name
def get_ip(hostname):
    return socket.gethostbyname(hostname)

def get_open_port():
    soc = socket.socket()
    soc.bind(("",0))
    # Get open port
    port = soc.getsockname()[1]
    soc.close()
    return port

def client_thread(conn, ip, port, ip_list, ring, disk_list, user):
    # Receive and send message
    while True:
        #connected?
        data = conn.recv(32)
        print('Received: {!r}'.format(data))

        if data == b'download':
            print('download file')

            # Receiving file name
            file = conn.recv(32)
            print('Received: ', file.decode())

            # hash file name and get disk
            disk_download = ring.download_file(str(file))
            print('Disks: ', disk_download)

            for disk in disk_download:
                #will be used for P2
                if disk == -1:
                    print('File does not exist')
                    break;
                else:
                    print('Sending: ', disk_list[disk])
                    conn.sendall(disk_list[disk].encode())
            print('Finished downloading file')
        elif data == b'upload':
            print('upload file')

            #receiving file name
            file = conn.recv(32)
            print('Received: ', file.decode())

            #hash file name and choose disk
            disk_add = ring.add_file(str(file))
            print('Disks: ', disk_add)

            dir, f = file.decode().split('/')
            print('dir name: ', dir)

            client_path = '/tmp/' + user + '/'
            print ('Full path: ', client_path)

            # send main and replica disk
            for disk in disk_add:
                print('Sending: ', disk_list[disk])
                create_dir(str(disk_list[disk]), user, client_path, dir)
                conn.sendall(disk_list[disk].encode())

            print('Finished uploading file')
        elif data == b'list':
            print('list files')
            print(ip_list)
            ipstring = ' '.join(ip_list)
            conn.sendall(ipstring.encode())
            print('list done')
        elif data == b'delete':
            print('Delete file')

            # Receiving file name
            file = conn.recv(32)
            print('Received: ', file.decode())

            # hash file name and get disk
            disk_download = ring.download_file(str(file))
            print('Disks: ', disk_download)

            for disk in disk_download:
                #will be used for P2
                if disk == -1:
                    print('File does not exist')
                    break;
                else:
                    print('Sending: ', disk_list[disk])
                    conn.sendall(disk_list[disk].encode())
            print('Finished downloading file')
        elif data == b'add':
            print('Add partition')

            # Receiving disk
            disk = conn.recv(32)
            print('Received: ', disk.decode())

            idx = len(disk_list)
            disk_list[idx] = disk.decode()
            print(disk_list)
            ring.add_partition(idx)
        elif data == b'remove':
            print('Remove partition')

            # Receiving disk
            disk = conn.recv(32)
            print('Received: ', disk.decode())

            disk_key = -1
            for key, value in disk_list.items():
                if value == disk.decode():
                    print('Key: ', key)
                    disk_key = key
            #ring.get_main_file(disk_key)
            ring.remove_partition(disk_key)

        elif data:
            print('Sending: You are connected')
            conn.sendall(b'You are connected')
        else:
            print('No more data received')
            break

    # Close connection
    print('Communication complete. Closing connection: ' + ip + ':' + port)
    conn.close()

def create_dir(ip, client, client_path, user):
    print('Creating directory if doesnt exist')
    ssh_cmd = 'ssh %s@%s mkdir %s' % (client, ip, client_path)
    proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE)
    proc.wait()
    user_path = client_path + user
    ssh_cmd = 'ssh %s@%s mkdir %s' % (client, ip, user_path)
    proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE)
    proc.wait()
    proc.kill()

def gen_key(user):
    private_ssh_dir = '/home/%s/.ssh' % (user)
    ssh_cmd = 'ssh-keygen -t rsa -N "" -f ' + private_ssh_dir + '/id_rsa'

    if not os.path.exists(private_ssh_dir):
        print('Creating directory: ', private_ssh_dir)
        os.makedirs(private_ssh_dir)

    # Generate key
    if 'id_rsa' in os.listdir(private_ssh_dir):
        print('Key exists.')
    else:
        # Generate private key
        proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE)
        proc.kill()
        #subprocess.call(ssh_cmd, shell=True)

def push_key(ip, user):
    ssh_cmd = 'ssh -q -o StrictHostKeyChecking=no ' + user + '@' + ip
    #subprocess.call(ssh_cmd, shell=False)
    proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE)
    proc.kill()
    #subprocess.call('exit', shell=True)

def start_server(arguments):
    user = os.getlogin()

    partition_power = arguments[0]

    client_list = arguments[1:]
    print('List of clients: ' + ''.join(client_list))
    size = len(client_list)
    print('Size: ', size)

    # create hash ring
    ring = construct_ring(int(partition_power), 2, len(client_list))

    ip_list = list()
    # Loop list and determine if client provided is an ip.
    for client in client_list:
        if check_ip(client):
            ip_list.append(client)
        else:
            ip_list.append(get_ip(client))
    print('The IP addresses are: ' + ''.join(ip_list))

    print('Generating ssh key')
    gen_key(user)

    disk_list = {}
    for idx, ip in enumerate(ip_list):
        push_key(ip, user)
        disk_list[idx] = ip
        print('disk_list: ', disk_list)

    print ('Starting server on local machine')
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print ('Socket created')

    host = socket.gethostname()
    print('Host is: %s' % host)

    host_ip = get_ip(host)
    print('Host ip address is: %s' % host_ip)

    port = get_open_port()
    print('Available port: %s' % port)

    # Binding the socket to the host and an open port.
    try:
        soc.bind(('', port))
        print('Socket bind complete')
    except socket.error as err:
        print('Bind failed. Error: ' + str(err))
        sys.exit()

    # Listen for clients, up to 8.
    print('Starting to listen')
    soc.listen(8)

    while True:
        print('Waiting to connect')
        conn, addr = soc.accept()
        ip, port = str(addr[0]), str(addr[1])
        print('Connection from: ' + ip + ':' + port)
        try:
            Thread(target=client_thread, args=(conn, ip, port, ip_list, ring, disk_list, user)).start()
        except:
            print('Something broke')
            import traceback
            traceback.print_exc()

def main(argv):
    if len(argv) < 2:
        print('Not enough arguments. Need at least 1 client')
        sys.exit()
    if len(argv) > 5:
        print('Too many arguments have been passed.')
        print('Accepts first argument for partition size, and next four are clients.')
        sys.exit()

    print('STARTING SERVER!!!')
    start_server(argv)

if __name__ == '__main__':
    main(sys.argv[1:])
