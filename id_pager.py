import os
from collections.abc import Generator, Iterable, Callable
from typing import Any
import requests
import sys
from itertools import islice
import json
import time

# TODO: Need to implement error handling for graphql response

def _chunked_id(file_path: str, checkpoint: str = None) -> Generator[list[str], None, None]:
    
    """
    Takes a file with node ids and yields out batches of 250 ids a time starting the id at checkpoint (optional).
    
    `file_path`: File path to file containing line delimited ids.
    `checkpoint`: Id from which to start reading.
    """
    
    with open(file_path) as f:
        if checkpoint:
            checkpoint = ''.join([_ for _ in checkpoint if _.isdigit()])
            line = f.readline()
            while checkpoint not in line:
                line = f.readline()
                if line == '':
                    f.seek(0)
                    break
        while True:
            lines = list(map(str.strip, islice(f, 250)))
            yield lines
            if not lines:
                break

def backoff(func: Callable) -> Callable:
    def wrapper(*args, **kwargs):
        r = func(*args, **kwargs)
        try:
            r.raise_for_status()
            return r
        except requests.HTTPError:
            if r.status_code == 429:
                time.sleep(1)
            else: 
                r.raise_for_status()
    return wrapper

class ApiSession():
    def __init__(self, url, token):
        self.url = url
        self.token = token
        self.headers = {'Content-Type' : 'application/json', 'X-Shopify-Access-token' : token}
        self.session = requests.Session()

    @backoff
    def get_nodes(self, query_str: str, ids: list[str], **kwargs) -> requests.Response:
        
        """
        Returns nodes by id from api
        
        parameters:
            `ids`: data of ids to fetch (max 250)
            `query`: the graphql query_str
            `**kwargs`: additional variables for the graphql query
        """
        variables = dict(id=ids,  **kwargs)
        json_body = {'query' : query_str, 'variables' : variables}
        
        r = self.session.post(url=self.url, headers=self.headers, json=json_body)
        return r
    

def main():
    query_str = '''
        query ($id: [ID!]!, $first: Int, $agreements_cursor: String, $sales_cursor: String) {
            nodes(ids: $id) {
                ... on Order {
                    id
                    createdAt
                    updatedAt
                    agreements(first: $first, after: $agreements_cursor) {
                        edges {
                            node {
                                id
                                happenedAt
                                reason
                                sales(first: $first, after: $sales_cursor) {
                                    edges {
                                        node {
                                            id
                                            actionType
                                            lineType
                                            quantity
                                            totalAmount {
                                                shopMoney {
                                                    amount
                                                }
                                            }
                                            taxes {
                                                amount {
                                                    shopMoney {
                                                        amount
                                                    }
                                                }
                                            }
                                            totalDiscountAmountAfterTaxes {
                                                shopMoney {
                                                    amount
                                                }
                                            }
                                            totalDiscountAmountBeforeTaxes {
                                                shopMoney {
                                                    amount
                                                }
                                            }
                                            totalTaxAmount {
                                                shopMoney {
                                                    amount
                                                }
                                            }
                                        }
                                    }
                                    pageInfo {
                                        hasNextPage
                                    }
                                }
                                app {
                                    id
                                }
                            }
                        }
                        pageInfo {
                            hasNextPage
                        }
                    }
                }
            }
        }
    '''
    headers = {'Content-Type' : 'application/json', 'X-Shopify-Access-token' : token}

    id_chunks = _chunked_id(file_path, checkpoint)
    session = ApiSession(url, token=token)
    count = 0
    with open('response', 'a+') as f:
        for ids in id_chunks:
            r = session.get_nodes(query_str, ids=ids, first=250)
            content = json.loads(r.text)

            id = content['data']['node']['id']
            print(f'id: {id} count')
            for node in content['data']['nodes']:
                json.dump(node, f)
                f.write('\n')
            count += 250
            

if __name__ == '__main__':
    try:
        args = sys.argv[1:]
        url = args[0]
        token = args[1]
        file_path = args[2]
        if len(args) == 4:
            checkpoint = args[3]
        else:
            checkpoint = None

        main()
    except FileNotFoundError:
        raise FileNotFoundError(f'file or filepath "{file_path}" is invalid')
    except IndexError:
        raise
    except Exception:
        raise