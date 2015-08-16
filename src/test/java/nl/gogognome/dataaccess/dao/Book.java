package nl.gogognome.dataaccess.dao;

class Book {

    enum Genre {
        THRILLER, FANTASY
    }

    private long id;
    private String title;
    private Genre genre;

    private long authorId;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setGenre(Genre genre) { this.genre = genre; }

    public Genre getGenre() { return genre; }

    public long getAuthorId() {
        return authorId;
    }

    public void setAuthorId(long authorId) {
        this.authorId = authorId;
    }
}
