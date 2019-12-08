from setuptools import setup, find_packages


long_description='Convert Canadian Line P data into Exchange format. Data '\
                 'found at https://www.waterproperties.ca/linep/cruises.php' 

setup(
    name='Canadian Line P data to Exchange',
    version='1.0',
    description='Convert Canadian Line P data into exchange format.',
    author='Lynne Merchant',
    author_email='lmerchant@ucsd.edu',
    url='https://github.com/lmerchant/convert_line_p_to_exchange',
    keywords='Exchange Format',
    packages=find_packages(),
    classifiers=[
        "Topic :: Converting Line P data to Exchange"
    ]
)
