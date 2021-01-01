import json, os, sys, time, threading


class data_store:
    def __init__(self, file_path=os.getcwd()):

        self.fp = file_path + '/database.json'
        self.fl = threading.Lock()
        self.dl = threading.Lock()

        try:
            file = open(self.fp, 'r')
            filedata = json.load(file)
            self.data = filedata
            file.close()

            if not self.checkfs():
                raise Exception('The data store size exceeds 1 GB')

            print('Database opened in the path  ' + self.fp)
        except:

            file = open(self.fp, 'w')
            self.data = {}
            self.ttldict = {}
            file.close()
            print('Database created in path ' + self.fp)

    def checkfs(self):

        self.fl.acquire()

        if os.path.getsize(self.fp) <= 1e+9:
            self.fl.release()
            return True
        else:
            self.fl.release()
            return False

    def verifykey(self, key):

        if type(key) == type(""):
            if len(key) > 32:
                raise Exception(
                    'Length of key is greater than 32. The length is ' +
                    str(len(key)))
            else:
                return True
        else:
            raise Exception('Key value is not a string.The key is of type: ' +
                            str(type(key)))
            return False

    def Create(self, key='', value='', ttl=None):

        self.verifykey(key)

        if key == '':
            raise Exception('No key was given.')

        if value == '':
            value = None

        if sys.getsizeof(value) > 16000:
            raise Exception("The value size exceeds 16KB size limit.")

        if not self.checkfs():
            raise Exception('The data store size exceeds 1 GB')

        self.dl.acquire()
        if key in self.data.keys():
            self.dl.release()
            raise Exception('Key is already there.')

        if ttl is not None:
            ttl = int(time.time()) + abs(int(ttl))

        tempdict = {'value': value, 'ttl': ttl}
        self.data[key] = tempdict

        self.fl.acquire()
        json.dump(self.data, fp=open(self.fp, 'w'), indent=2)

        self.fl.release()
        self.dl.release()

        print('Value added')

    def Read(self, key=''):

        self.verifykey(key)

        if key == '':
            raise Exception('Expecting a key to be read.')

        self.dl.acquire()

        if key in self.data.keys():
            pass
        else:
            self.dl.release()
            raise Exception('Key not found ')

        ttl = self.data[key]['ttl']

        if not ttl:
            ttl = 0

        if (time.time() < ttl) or (ttl == 0):
            self.dl.release()
            return json.dumps(self.data[key]['value'])

        else:
            self.dl.release()
            raise Exception("Key's Time-To-Live has expired. Unable to read.")

    def Delete(self, key=''):

        self.verifykey(key)

        if key == '':
            raise Exception('Expecting a key to be read.')

        self.dl.acquire()

        if key in self.data.keys():
            pass
        else:
            self.dl.release()
            raise Exception('Key not found in database')

        ttl = self.data[key]['ttl']

        if not ttl:
            ttl = 0

        if time.time() < ttl or (ttl == 0):

            self.data.pop(key)

            self.fl.acquire()
            file = open(self.fp, 'w')
            json.dump(self.data, file)

            self.fl.release()
            self.dl.release()

            print("Key-value pair deleted")
            return
        else:
            self.dl.release()
            raise Exception(
                "Key's Time-To-Live has expired. Unable to delete.")

    def ClearAll(self):

        ch = input('Are you sure you want to clear the database ? (y/n)')

        if ch == 'y':
            self.fl.acquire()
            file1 = open(self.fp, 'w')
            file1.close()
            print('Data cleared')
            self.fl.release()

            self.dl.acquire()
            self.data.clear()
            self.dl.release()

        else:
            print('Data not cleared')

    def DisplayAll(self):

        self.dl.acquire()

        if len(self.data.keys()) == 0:

            self.dl.release()
            raise Exception("Unable to display. Database is empty")

        else:

            data = list(self.data.items())
            data = dict([[key, values['value']] for key, values in data])
            self.dl.release()

            print(data)
