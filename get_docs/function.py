import json
import boto3
import requests
import os


api_key = os.environ['api_key']
year = 2008
url = os.environ['url']


s3 = boto3.client('s3')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

def get_authors(authors):
    author =  ';'.join(map(
        lambda creator: creator['creator'].strip(),
        authors
    ))

    return author if author != '' else None


def get_record_pdf(record_urls):
    return next(
        map(
            lambda item: item['value'],
            filter(
                lambda url: url['format'] == 'pdf',
                record_urls
            )
        ),
        None
    )


def parseEmpty(data):
    clean = data.strip()
    return clean if clean != '' else None


def record_parser(record):
    return {
        'title': parseEmpty(record['title']),
        'type': parseEmpty(record['contentType']),
        'authors': get_authors(record['creators']),
        'pdf': get_record_pdf(record['url']),
        'publication': parseEmpty(record['publicationName']),
        'source': 'Springer Open',
        'issn': parseEmpty(record['issn']),
        'eissn': parseEmpty(record['eissn']),
        'publisher': parseEmpty(record['publisher']),
        'date': parseEmpty(record['publicationDate']),
        'genre': parseEmpty(record['genre']),
        'Abstract': parseEmpty(record['abstract']),
        'Category': parseEmpty(record['articleCategory']),
        'id': '{}.pdf'.format(parseEmpty(record['title']).replace(' ', '-'))
    }


def lambda_handler(event, context):
    r = requests.get(url=url, params={
        'q': 'year:{}'.format(year),
        'api_key': api_key
    })
    data = r.json()
    paper_list = []
    for record in data['records']:
        paper_list.append(record_parser(record))
    paper_list.append('DONE')
    return paper_list
