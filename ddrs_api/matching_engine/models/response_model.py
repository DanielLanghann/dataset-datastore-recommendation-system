from django.db import models


class MatchingResponse(models.Model):
    """
    Stores AI matching responses incl. datasets and recommended datastores
    """

    # Auto generated fields
    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    request_id = models.ForeignKey(
        "matching_engine.MatchingRequests",
        on_delete=models.CASCADE,
        related_name="request_id",
        null=True,
        blank=True,
        help_text="The related request based on this response.",
    )
    result = models.JSONField(
        help_text="""The result of the request as JSON response containing a list with
        - Dataset IDs
        - Matched datastore IDs
        - Reasons for recommendations
        - Confidence scores."""
    )
    model = models.CharField(
        max_length=100,
        help_text="The used LLM for this response.",
        default="llama3.1:8b",
    )

    description = models.TextField(
        blank=True, help_text="Optional description of the matching request"
    )

    class Meta:
        db_table = "ai_matching_response"
        ordering = ["-created_at"]
        verbose_name = "AI Matching Response"
        verbose_name_plural = "AI Matching Responses"

    def __str__(self):
        return f"Response {self.id} - {self.created_at.strftime('%Y-%m-%d %H:%M')}"

    def get_datasets_count(self):
        """
        Returns the count of datasets in the result JSON.
        """
        if not self.result:
            return 0

        try:
            # case 1
            if isinstance(self.result, dict):
                if "datasets" in self.result:
                    datasets = self.result["datasets"]
                    return len(datasets) if isinstance(datasets, list) else 0
                elif "dataset_ids" in self.result:
                    dataset_ids = self.result["dataset_ids"]
                    return len(dataset_ids) if isinstance(dataset_ids, list) else 0
                # case 2
                elif "dataset_id" in self.result:
                    return 1

            # case 3
            elif isinstance(self.result, list):
                count = 0
                for item in self.result:
                    if isinstance(item, dict) and "dataset_id" in item:
                        count += 1
                    elif isinstance(item, (int, str)):
                        count += 1
                return count

        except (TypeError, AttributeError, KeyError):
            pass

        return 0

    def get_datastores_count(self):
        """
        returns the count of datastores in the result JSON.
        """
        if not self.result:
            return 0

        try:
            # case 1
            if isinstance(self.result, dict):
                if "datastores" in self.result:
                    datastores = self.result["datastores"]
                    return len(datastores) if isinstance(datastores, list) else 0
                elif "datastore_ids" in self.result:
                    datastore_ids = self.result["datastore_ids"]
                    return len(datastore_ids) if isinstance(datastore_ids, list) else 0
                elif "matched_datastores" in self.result:
                    matched_datastores = self.result["matched_datastores"]
                    return (
                        len(matched_datastores)
                        if isinstance(matched_datastores, list)
                        else 0
                    )
                elif "matched_datastore_ids" in self.result:
                    matched_datastore_ids = self.result["matched_datastore_ids"]
                    return (
                        len(matched_datastore_ids)
                        if isinstance(matched_datastore_ids, list)
                        else 0
                    )
                # case 2
                elif "datastore_id" in self.result:
                    return 1

            # case 3
            elif isinstance(self.result, list):
                # count items that have datastore_id
                count = 0
                for item in self.result:
                    if isinstance(item, dict) and (
                        "datastore_id" in item or "matched_datastore_id" in item
                    ):
                        count += 1
                return count

        except (TypeError, AttributeError, KeyError):
            pass

        return 0
