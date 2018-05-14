from elasticsearch import Elasticsearch, helpers

from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
    # Connect to localhost:9200 by default.
    es = Elasticsearch()

    es.indices.delete(index=INDEX_NAME, ignore=404)
    es.indices.create(
        index=INDEX_NAME,
        body={
            'mappings': {},
            'settings': {},
        },
    )

    products = all_products()

    ''' bulk instead of creating one by one
    for product in products: 
        index_product(es, product)
    '''
    bulk_index_products(es, products)


def index_product(es, product: ProductData):
    """Add a single product to the ProductData index."""

    es.create(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        id=product.id,
        body={
            "name": product.name,
            "image": product.image,
        }
    )

    # Don't delete this! You'll need it to see if your indexing job is working,
    # or if it has stalled.
    print("Indexed {}".format(product.name))


def bulk_index_products(es, products):
    def format_bulk_action(product: ProductData):
        return {
            '_op_type': 'create',
            '_index': INDEX_NAME,
            '_type': DOC_TYPE,
            '_id': product.id,
            '_source': {
                'name': product.name,
                'image': product.image,
            }
        }
    # this is not efficient, for workshop practice only
    actions = [format_bulk_action(product) for product in products]
    helpers.bulk(es, actions)


if __name__ == '__main__':
    main()
