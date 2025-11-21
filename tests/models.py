from django.db import models


class ExamplePaginationModel(models.Model):
    # Don't use an auto field because we can't reset
    # sequences and that's needed for this test
    id = models.IntegerField(primary_key=True)
    field = models.IntegerField()
    timestamp = models.IntegerField()
