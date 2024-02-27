'''
Выгрузка сгенерированных тестовых данных в коллекции ArangoDB.
'''


from arango import ArangoClient
import json


def arango_connect():
    return ArangoClient().db(
        name='tx_fraud',
        username='egor_yakovenko',
        password='1234',
        verify=True,
        verify_certificate=False
    )


def create_account_collection():
    adb.delete_collection('account', ignore_missing=True)
    account = adb.create_collection(
        name='account',
        edge=False,
        key_generator='autoincrement',
        key_increment=1,
        key_offset=0,
    )

    with open(r'arango_input/accounts.json', 'r') as account_json:
        acc = json.load(account_json)

        account.insert_many(acc)


def create_transaction_collection():
    adb.delete_collection('transaction', ignore_missing=True)
    transaction = adb.create_collection(
        name='transaction',
        edge=True,
        key_generator='autoincrement',
        key_increment=1,
        key_offset=0,
    )

    with open(r'arango_input/transactions.json', 'r') as transactions_json:
        transactions = json.load(transactions_json)

        for tx in transactions:
            tx['_to'] = 'account/' + str(tx['sender'] + 1)
            tx['_from'] = 'account/' + str(tx['receiver'] + 1)

            transaction.insert(tx)

adb = arango_connect()

create_account_collection()
create_transaction_collection()
