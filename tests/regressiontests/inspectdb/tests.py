from __future__ import absolute_import

from StringIO import StringIO

from django.core.management import call_command
from django.test import TestCase, skipUnlessDBFeature
from .models import People

def qname_filter(qn):
    return 'inspectdb_' in qn.table

class InspectDBTestCase(TestCase):

    def test_stealth_table_name_filter_option(self):
        out = StringIO()
        # Lets limit the introspection to tables created for models of this
        # application
        call_command('inspectdb',
                     qname_filter=qname_filter,
                     stdout=out)
        error_message = "inspectdb has examined a table that should have been filtered out."
        # contrib.contenttypes is one of the apps always installed when running
        # the Django test suite, check that one of its tables hasn't been
        # inspected
        self.assertNotIn("class DjangoContentType(models.Model):", out.getvalue(), msg=error_message)
        out.close()

    @skipUnlessDBFeature('can_introspect_foreign_keys')
    def test_attribute_name_not_python_keyword(self):
        from django.db import connection
        _, tbl, _ = connection.introspection.qname_converter(
            People._meta.qualified_name)
        mname = ''.join(t.title() for t in tbl.split('_'))
        out = StringIO()
        # Lets limit the introspection to tables created for models of this
        # application
        call_command('inspectdb',
                     qname_filter=qname_filter,
                     stdout=out)
        error_message = "inspectdb generated an attribute name which is a python keyword"
        self.assertNotIn("from = models.ForeignKey(%s)" % mname, out.getvalue(), msg=error_message)
        # As InspectdbPeople model is defined after InspectdbMessage, it should be quoted
        self.assertIn("from_field = models.ForeignKey('%s')" % mname, out.getvalue())
        self.assertIn("people_pk = models.ForeignKey(%s, primary_key=True)" % mname,
            out.getvalue())
        self.assertIn("people_unique = models.ForeignKey(%s, unique=True)" % mname,
            out.getvalue())
        out.close()

    def test_digits_column_name_introspection(self):
        """Introspection of column names consist/start with digits (#16536/#17676)"""
        out = StringIO()
        # Lets limit the introspection to tables created for models of this
        # application
        call_command('inspectdb',
                     qname_filter=qname_filter,
                     stdout=out)
        error_message = "inspectdb generated a model field name which is a number"
        self.assertNotIn("    123 = models.CharField", out.getvalue(), msg=error_message)
        self.assertIn("number_123 = models.CharField", out.getvalue())

        error_message = "inspectdb generated a model field name which starts with a digit"
        self.assertNotIn("    4extra = models.CharField", out.getvalue(), msg=error_message)
        self.assertIn("number_4extra = models.CharField", out.getvalue())

        self.assertNotIn("    45extra = models.CharField", out.getvalue(), msg=error_message)
        self.assertIn("number_45extra = models.CharField", out.getvalue())
