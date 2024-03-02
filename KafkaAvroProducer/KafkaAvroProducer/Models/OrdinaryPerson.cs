namespace KafkaAvroProducer.Models
{
    public record OrdinaryPerson(
        int Id,
        string Name,
        int Age,
        string Email,
        string Address
    );
}
