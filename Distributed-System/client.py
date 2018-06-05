import sys
import socket
import os
import subprocess
import shlex

def check_ip(addr):
    try:
        socket.inet_aton(addr)
        print ('Valid IP address: %s' % addr)
        return True
    except socket.error:
        print ('Not IP address: %s' % addr)
        return False

def get_ip(hostname):
    print ('Get ip address with hostname: %s' % hostname)
    return socket.gethostbyname(hostname)

def download_file(conn, file_name):
    #send command
    print('Sending: download')
    conn.sendall(b'download')

    #send file_name
    print('Sending: ', file_name)
    conn.sendall(file_name.encode())

    # main disk ip address
    main_disk = conn.recv(32)
    print('Received main: ', main_disk)

    # replica disk ip address
    replica_disk = conn.recv(32)
    print('Received replica: ', replica_disk)

    file = file_name.split('/')[-1]
    print('file name: ', file)

    full_path = '/tmp/' + os.environ['user'] + '/' + file_name
    print ('Full path: ', full_path)

    print('Check if file exists in Main')
    ssh_cmd = 'ssh %s@%s ls -l %s' % (os.environ['user'], main_disk.decode(), full_path)
    proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    proc.wait()
    out, err = proc.communicate()
    print(out)

    if out == b'':
        print( 'Main disk does not have the file')
        print('Check replica')
        print('Check if file exists in Main')
        ssh_cmd = 'ssh %s@%s ls -l %s' % (os.environ['user'], replica_disk.decode(), full_path)
        proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        proc.wait()
        out, err = proc.communicate()
        print(out)
        if out == b'':
            print('Replica disk does not have the file')
        else:
            get_file = 'scp -B ' + os.environ['user'] + '@' + replica_disk.decode() + ':' + full_path + ' .'
            print('scp: ', get_file)
            os.system(get_file)
    else:
        get_file = 'scp -B ' + os.environ['user'] + '@' + main_disk.decode() + ':' + full_path + ' .'
        print('scp: ', get_file)
        os.system(get_file)

def send_file(conn, file_name):
    #send command
    print('Sending: upload')
    conn.sendall(b'upload')

    #send file_name
    print('Sending: ', file_name)
    conn.sendall(file_name.encode())

    # host name: disk where the file will be sent
    main_disk = conn.recv(32)
    print('Received main: ', main_disk)

    replica_disk = conn.recv(32)
    print('Received replica: ', replica_disk)

    dir, file = file_name.split('/')
    print('file name: ', file)

    full_path = '/tmp/' + os.environ['user'] + '/' + dir
    print ('Full path: ', full_path)

    #Need to check on remote machine...
    if not os.path.exists(full_path):
        print('Creating directory: ', full_path)
        os.makedirs(full_path)

    send_to_main = 'scp -B ' + file + ' ' + os.environ['user'] + '@' + main_disk.decode() + ':' + full_path + '/' + file
    print('scp: ', send_to_main)
    os.system(send_to_main)

    send_to_replica = 'scp -B ' + file + ' ' + os.environ['user'] + '@' + replica_disk.decode() + ':/tmp/' + os.environ['user'] + '/' + file_name
    print('scp: ', send_to_replica)
    os.system(send_to_replica)

def list_files(conn, user):
    print('Sending: list')
    conn.sendall(b'list')

    print('Receiving ips')
    ip_list = conn.recv(1024)

    ip_arr = ip_list.decode()
    ip_arr = ip_arr.split(' ')

    full_path = '/tmp/' + os.environ['user'] + '/' + user
    print ('Full path: ', full_path)

    for ip in ip_arr:
        ssh_cmd = 'ssh %s@%s ls -lrt %s' % (os.environ['user'], ip, full_path)
        print(ssh_cmd)
        proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE)
        proc.wait()

def delete_file(conn, file_name):
    #send command
    print('Sending: delete')
    conn.sendall(b'delete')

    #send file_name
    print('Sending: ', file_name)
    conn.sendall(file_name.encode())

    # main disk ip address
    main_disk = conn.recv(32)
    print('Received main: ', main_disk)

    # replica disk ip address
    replica_disk = conn.recv(32)
    print('Received replica: ', replica_disk)

    file = file_name.split('/')[-1]
    print('file name: ', file)

    full_path = '/tmp/' + os.environ['user'] + '/' + file_name
    print ('Full path: ', full_path)

    print('Delete file from Main')
    ssh_cmd = 'ssh %s@%s rm %s' % (os.environ['user'], main_disk.decode(), full_path)
    print(ssh_cmd)
    proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE)
    proc.wait()

    print('Delete file from replica')
    ssh_cmd = 'ssh %s@%s rm %s' % (os.environ['user'], replica_disk.decode(), full_path)
    print(ssh_cmd)
    proc = subprocess.Popen(shlex.split(ssh_cmd), shell=False, stdin=subprocess.PIPE)
    proc.wait()

def add_disk(conn, disk):
    print('Sending: add')
    conn.sendall(b'add')

    print('Sending: ', disk)
    conn.sendall(disk.encode())

def remove_disk(conn, disk):
    print('Sending: remove')
    conn.sendall(b'remove')

    print('Sending: ', disk)
    conn.sendall(disk.encode())

def connect_client(server_name, port, command, argument):

    # Checking ip address of client and open port.
    if check_ip(server_name):
        server_ip = server_name
    else:
        server_ip = get_ip(server_name)

    # Creating a socket
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Created socket')

    # Connect to server
    conn.connect((server_ip, port))
    print('Connected')

    try:
        # Sending data Am I connected?
        message = b'Am I connected?'
        print('Sending: {!r}'.format(message))
        conn.sendall(message)

        # Receiving data You are connected
        data = conn.recv(32)
        print('Received: {!r}'.format(data))

        if command == 'download':
            print('Downloading file')
            download_file(conn, argument)
            print('done downloading')
        elif command == 'upload':
            print('Uploading file')
            send_file(conn, argument)
            print('done uploading')
        elif command == 'list':
            print('List files')
            list_files(conn, argument)
        elif command == 'delete':
            print('Delete file')
            delete_file(conn, argument)
            print('File deleted')
        elif command == 'add':
            print ('Adding partition')
            add_disk(conn, argument)
        elif command == 'remove':
            print('Removing partition')
            remove_disk(conn, argument)

    finally:
        # Close connection
        print('Communication complete. Closing connection.')
        conn.close()

def main(argv):
    if len(argv) < 2:
        print('Not enough arguments. Need at least 1 client')
        sys.exit()
    if len(argv) > 2:
        print('Too many arguments have been passed.')
        sys.exit()

    server_name = str(argv[0])
    port = int(argv[1])

    #os.environ['user'] = 'acheong'
    print('Environment Variable: ', os.environ['user'])

    user_input = input('Enter command: ')
    command, argument = user_input.split(' ')

    connect_client(server_name, port, command, argument)

if __name__ == '__main__':
    main(sys.argv[1:])
