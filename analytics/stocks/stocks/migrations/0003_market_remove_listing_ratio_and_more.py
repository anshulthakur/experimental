# Generated by Django 4.0.1 on 2022-03-29 21:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('stocks', '0002_listing_date_idx'),
    ]

    operations = [
        migrations.CreateModel(
            name='Market',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=255)),
            ],
        ),
        migrations.RemoveField(
            model_name='listing',
            name='ratio',
        ),
        migrations.RemoveField(
            model_name='listing',
            name='spread_close_open',
        ),
        migrations.RemoveField(
            model_name='listing',
            name='spread_high_low',
        ),
        migrations.AlterField(
            model_name='stock',
            name='security',
            field=models.BigIntegerField(default=0),
        ),
        migrations.AddIndex(
            model_name='stock',
            index=models.Index(fields=['isin'], name='isin_idx'),
        ),
        migrations.AddIndex(
            model_name='stock',
            index=models.Index(fields=['sid'], name='sid_idx'),
        ),
        migrations.AddField(
            model_name='stock',
            name='market',
            field=models.ForeignKey(default=None, null=True, on_delete=django.db.models.deletion.SET_NULL, to='stocks.market'),
        ),
    ]