'''
Генерация тестовых данных для выгрузки в ArangoDB.

alerted_flg (флаг подозрительности операции) будет проставляться:
    - модулем в процессе выгрузки;
    - после выгрузки с помощью AQL-запросов (экспертных правил);
    - с помощью графовой нейронной сети, представленной в директории dgl_network.
'''


from random import random, randint, choice
from numpy.random import normal
import json

from params import *


def generate_accounts():
    '''
    Генерация аккаунтов
    '''
    accounts = list()
    sums = normal(
        INITIAL_BALANCE_MEAN,
        INITIAL_BALANCE_STD,
        NUM_ACCOUNTS
    )

    for i in range(NUM_ACCOUNTS):
        account_type = 'DEBIT' if random() < DEBIT_CHANCE else 'CREDIT'
        account_creation_dttm = randint(MIN_CREATION_HR, NUM_HOURS)

        accounts.append({
            'account_id' : i,
            'start_balance' : round(sums[i]),
            'account_type' : account_type,
            'account_creation_dttm' : account_creation_dttm
        })

    with open('arango_input/accounts.json', 'w+') as file:
        json.dump(accounts, file)

    return accounts


def check_rule_tx_balance(tx_sum:int, tx_type:str) -> bool:
    '''
    Проверить транзакцию на выполнение первого экспертного правила:
    сумма транзакции не должна превышать 1000 для транзакций типа CASH_IN и CASH_OUT, и 2500 для TRANSFER.
    '''
    if (tx_type == 'CASH_IN' or tx_type == 'CASH_OUT') \
    and tx_sum > 1000:
        return True
    
    if tx_type == 'TRANSFER' \
    and tx_sum > 2500:
        return True
    
    return False


def check_rule_tx_credit_negative(tx_type:str, end_acc_balance:int):
    '''
    Проверить транзакцию на выполнение второго экспертного правила:
    транзакция CASH_OUT и TRANSFER не должна создавать отрицательный баланс на аккаунтах типа CREDIT.
    '''
    if end_acc_balance < 0 \
    and (tx_type == 'CASH_OUT' or tx_type == 'TRANSFER'):
        return True
    
    return False


def generate_transactions():
    '''
    Генерация случайного числа транзакций между аккаунтами
    '''
    accounts = generate_accounts()
    transactions = list()

    tx = 0

    for i in range(NUM_HOURS):
        NUM_TX = randint(MIN_TX_IN_ONE_HOUR, MAX_TX_IN_ONE_HOUR)

        created_acc = [acc for acc in accounts if acc['account_creation_dttm'] <= i]

        for _ in range(NUM_TX):
            sender = choice(created_acc)

            tx_type_random = random()
            if tx_type_random < TRANSFER_CHANCE:
                tx_type = 'TRANSFER'
            elif tx_type_random < TRANSFER_CHANCE + CASH_IN_CHANCE:
                tx_type = 'CASH_IN'
            else:
                tx_type = 'CASH_OUT'

            if tx_type == 'TRANSFER':
                while True:
                    receiver = choice(created_acc)
                    if receiver['account_id'] != sender['account_id']:
                        break
            else:
                receiver = sender

            if tx_type == 'CASH_IN':
                tx_amount = randint(1, 5000)
            elif sender['account_type'] == 'CREDIT' and sender['start_balance'] > CREDIT_NEGATIVE_LIMIT:
                tx_amount = randint(1, sender['start_balance'] - CREDIT_NEGATIVE_LIMIT)
            elif sender['start_balance'] > 0:
                tx_amount = randint(1, sender['start_balance'])
            else:
                continue

            is_alerted = check_rule_tx_balance(tx_amount, tx_type)

            if tx_type == 'TRANSFER':
                sender['start_balance'] -= tx_amount
                receiver['start_balance'] += tx_amount
            elif tx_type == 'CASH_IN':
                sender['start_balance'] += tx_amount
            elif tx_type == 'CASH_OUT':
                sender['start_balance'] -= tx_amount

            is_alerted |= check_rule_tx_credit_negative(tx_type, sender['start_balance'])

            transactions.append({
                'tx_id' : tx,
                'sender' : sender['account_id'],
                'receiver' : receiver['account_id'],
                'tx_type' : tx_type,
                'tx_amount' : tx_amount,
                'timestamp' : i,
                'is_alerted' : is_alerted,
                'is_fraud' : 0,
            })

            tx += 1

    with open('arango_input/transactions.json', 'w+') as file:
        json.dump(transactions, file)


generate_transactions()
