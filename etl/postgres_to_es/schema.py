import uuid
from typing import List, Optional
from dataclasses import dataclass


@dataclass
class UUIDModel:
    id: uuid.UUID


class PersonInFilm(UUIDModel):
    name: str


@dataclass
class ESMovies:
    id: uuid.UUID
    imdb_rating: Optional[float] = None
    genre: Optional[List[str]] = None
    title: Optional[str] = None
    description: Optional[str] = None
    director: Optional[List[str]] = None
    actors_names: Optional[List[str]] = None
    writers_names: Optional[List[str]] = None
    actors: Optional[List[PersonInFilm]] = None
    writers: Optional[List[PersonInFilm]] = None
    modified: Optional[str] = None
